//go:build integration

package recovery_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"jungle/internal/adapters/observability"
	"jungle/internal/adapters/postgres"
	brokersqs "jungle/internal/adapters/sqs"
	"jungle/internal/application"
	"jungle/internal/domain"
	"jungle/internal/platform/config"
	"jungle/internal/workers"
)

// All persistence and queue calls in these tests reach actual PostgreSQL and
// LocalStack. The test observer records successful broker calls, not simulated
// ones. Process exits are real and execute after the named durable window.
type fixture struct {
	pool                         *pgxpool.Pool
	store                        *postgres.Store
	broker                       *observedBroker
	api                          *awssqs.Client
	config                       config.Config
	inputURL, outputURL, deadURL string
}
type observedBroker struct {
	*brokersqs.Client
	pool        *pgxpool.Pool
	fastRedrive bool
}

func (b *observedBroker) Receive(ctx context.Context, max int) ([]brokersqs.Message, error) {
	messages, err := b.Client.Receive(ctx, max)
	if err != nil {
		return nil, err
	}
	for _, m := range messages {
		if _, err := b.pool.Exec(ctx, `INSERT INTO recovery_observation(operation,identity,payload,receive_count) VALUES('receive',$1,$2,$3)`, m.ID, m.Body, m.ReceiveCount); err != nil {
			return nil, err
		}
	}
	return messages, nil
}
func (b *observedBroker) Publish(ctx context.Context, id, aggregate string, payload []byte) error {
	if err := b.Client.Publish(ctx, id, aggregate, payload); err != nil {
		return err
	}
	_, err := b.pool.Exec(ctx, `INSERT INTO recovery_observation(operation,identity,payload) VALUES('publish',$1,$2)`, id, string(payload))
	return err
}
func (b *observedBroker) Visibility(ctx context.Context, receipt string, delay time.Duration) error {
	// Only speeds up retry timing in the poison-message test. ReceiveMessage,
	// ChangeMessageVisibility and native SQS redrive still execute for real.
	if b.fastRedrive {
		delay = 0
	}
	return b.Client.Visibility(ctx, receipt, delay)
}

// TestMain sets the broker credentials once for the whole binary. t.Setenv
// would do the same per test, but it forbids t.Parallel, and these tests are
// independent by construction: each owns a private schema and its own queues.
func TestMain(m *testing.M) {
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID":         "test",
		"AWS_SECRET_ACCESS_KEY":     "test",
		"AWS_EC2_METADATA_DISABLED": "true",
	} {
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
	os.Exit(m.Run())
}

var (
	gooseOnce sync.Once
	gooseMu   sync.Mutex
)

func newFixture(t *testing.T) *fixture {
	t.Helper()
	// Deliberately serial. These fixtures run on deadlines measured in
	// milliseconds -- a 2s visibility timeout, a 2s lease, a 1.5s drain -- and
	// they kill and respawn real processes. Run in parallel against a four-vCPU
	// broker they failed two runs in three, which for the crash-recovery proofs
	// is worse than being slow: a flaky proof of durability proves nothing.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dsn := env("TEST_DATABASE_URL", "postgres://jungle:jungle-local@localhost:5433/jungle?sslmode=disable")
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := "recovery_" + strings.ReplaceAll(uuid(t), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("cleanup isolated schema: %v", err)
		}
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	isolatedDSN := u.String()
	db, err := sql.Open("pgx", isolatedDSN)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	_, file, _, _ := runtime.Caller(0)
	migrationDir := filepath.Join(filepath.Dir(file), "../../migrations")
	// goose keeps its dialect and migration state in package variables, so
	// parallel fixtures must not drive it at the same time.
	gooseMu.Lock()
	var gooseErr error
	gooseOnce.Do(func() { gooseErr = goose.SetDialect("postgres") })
	if gooseErr == nil {
		gooseErr = goose.UpContext(ctx, db, migrationDir)
	}
	gooseMu.Unlock()
	if gooseErr != nil {
		db.Close()
		t.Fatal(gooseErr)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, isolatedDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE TABLE recovery_observation(sequence bigserial PRIMARY KEY,operation text NOT NULL,identity text NOT NULL,payload text NOT NULL,receive_count integer,created_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	endpoint := env("TEST_SQS_ENDPOINT", "http://localhost:4566")
	api := awssqs.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", ""))}, func(o *awssqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
	prefix := "recovery-" + uuid(t)
	urls := make(map[string]string)
	for _, suffix := range []string{"dead", "events-dead", "input", "output"} {
		result, err := api.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(prefix + "-" + suffix + ".fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "2"}})
		if err != nil {
			t.Fatal(err)
		}
		queueURL := rewrite(aws.ToString(result.QueueUrl), endpoint)
		urls[suffix] = queueURL
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := api.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)}); err != nil {
				t.Errorf("delete isolated queue: %v", err)
			}
		})
	}
	attributes, err := api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(urls["dead"]), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attributes.Attributes["QueueArn"], "maxReceiveCount": "5"})
	if _, err := api.SetQueueAttributes(ctx, &awssqs.SetQueueAttributesInput{QueueUrl: aws.String(urls["input"]), Attributes: map[string]string{"RedrivePolicy": string(redrive)}}); err != nil {
		t.Fatal(err)
	}
	// PublishBatch is 1 on purpose. These tests crash the publisher inside a
	// precise window and then assert which event reached the broker first; a
	// wider batch publishes concurrently, so several events land before the
	// process dies and "the first one" stops being a well-defined thing.
	c := config.Config{DatabaseURL: isolatedDSN, InstanceID: prefix, AWSRegion: "us-east-1", SQSEndpoint: endpoint, InputQueue: prefix + "-input.fifo", OutputQueue: prefix + "-output.fifo", InputDLQ: prefix + "-dead.fifo", OutputDLQ: prefix + "-events-dead.fifo", TrustedProviders: []string{"provider-a"}, Concurrency: 1, PublishBatch: 1, MaxConns: 20, ProcessTimeout: time.Second, VisibilityTimeout: 2 * time.Second, ShutdownTimeout: 1500 * time.Millisecond, Lease: 2 * time.Second, PollInterval: 20 * time.Millisecond, ReferenceTTL: time.Hour, ReferenceMaxAttempts: 12}
	b, err := brokersqs.New(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return &fixture{pool: pool, store: postgres.New(pool), broker: &observedBroker{Client: b, pool: pool}, api: api, config: c, inputURL: urls["input"], outputURL: urls["output"], deadURL: urls["dead"]}
}
func (f *fixture) engine(t *testing.T) *workers.Engine {
	t.Helper()
	engine := workers.New(f.store, f.broker, f.config, slog.New(slog.NewJSONHandler(io.Discard, nil)), observability.New())
	engine.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := engine.Stop(ctx); err != nil {
			t.Errorf("stop recovery engine: %v", err)
		}
	})
	return engine
}
func (f *fixture) wallet(t *testing.T) application.WalletView {
	t.Helper()
	money, err := domain.ParseMoney("100.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.store.CreateWallet(context.Background(), application.CreateWalletCommand{PlayerID: uuid(t), InitialBalance: money, CorrelationID: "recovery"})
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func (f *fixture) send(t *testing.T, body, group string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.api.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: aws.String(f.inputURL), MessageBody: aws.String(body), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(uuid(t))}); err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) crash(t *testing.T, mode string) {
	t.Helper()
	encoded, err := json.Marshal(f.config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashChildProcess$", "-test.count=1")
	command.Env = append(os.Environ(), "JUNGLE_RECOVERY_CHILD="+mode, "JUNGLE_RECOVERY_CONFIG="+string(encoded))
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 23 {
		t.Fatalf("expected genuine exit23 in %s window: %v output=%s", mode, err, output)
	}
}

func TestCrashChildProcess(t *testing.T) {
	mode := os.Getenv("JUNGLE_RECOVERY_CHILD")
	if mode == "" {
		return
	}
	var c config.Config
	if err := json.Unmarshal([]byte(os.Getenv("JUNGLE_RECOVERY_CONFIG")), &c); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, c.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	b, err := brokersqs.New(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	hooks := workers.Hooks{}
	if mode == "commit" {
		hooks.AfterCommit = func() { os.Exit(23) }
	} else if mode == "publish" {
		hooks.AfterPublish = func() { os.Exit(23) }
	} else {
		t.Fatal("unsupported crash window")
	}
	engine := workers.New(postgres.New(pool), &observedBroker{Client: b, pool: pool}, c, slog.New(slog.NewJSONHandler(io.Discard, nil)), observability.New()).WithHooks(hooks)
	engine.Start()
	<-ctx.Done()
	t.Fatal("failure window not reached before child deadline")
}

func TestRecoveryConsumerCrashAfterCommitBeforeDelete(t *testing.T) {
	f := newFixture(t)
	wallet := f.wallet(t)
	messageID := "message-" + uuid(t)
	body := fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":"2026-09-19T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"recovery-bet","idempotencyKey":"recovery-key","playerId":%q,"walletId":%q,"roundId":"recovery-round","gameId":"recovery-game","kind":"BET","money":{"amount":"80.00","currency":"BRL"}}}`, messageID, wallet.PlayerID, wallet.ID)
	f.send(t, body, wallet.ID)
	f.crash(t, "commit")
	assertFinancialState(t, f, wallet.ID, 2000, 1, 1)
	var complete bool
	if err := f.pool.QueryRow(context.Background(), `SELECT completed_at IS NOT NULL FROM inbox WHERE message_id=$1`, messageID).Scan(&complete); err != nil || !complete {
		t.Fatalf("inbox not durable before process death: %v", err)
	}
	f.config.InstanceID += "-restarted"
	f.engine(t)
	eventually(t, 15*time.Second, func() bool {
		var receives, maxCount int
		err := f.pool.QueryRow(context.Background(), `SELECT count(*),COALESCE(max(receive_count),0) FROM recovery_observation WHERE operation='receive'`).Scan(&receives, &maxCount)
		return err == nil && receives >= 2 && maxCount >= 2
	})
	eventually(t, 10*time.Second, func() bool {
		r, err := f.api.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(f.inputURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
		return err == nil && r.Attributes["ApproximateNumberOfMessages"] == "0" && r.Attributes["ApproximateNumberOfMessagesNotVisible"] == "0"
	})
	assertFinancialState(t, f, wallet.ID, 2000, 1, 1)
}

func TestRecoveryPublisherCrashAfterPublishBeforeMark(t *testing.T) {
	f := newFixture(t)
	f.wallet(t)
	f.crash(t, "publish")
	var eventID, firstPayload string
	if err := f.pool.QueryRow(context.Background(), `SELECT identity,payload FROM recovery_observation WHERE operation='publish' ORDER BY sequence LIMIT 1`).Scan(&eventID, &firstPayload); err != nil {
		t.Fatal(err)
	}
	var published bool
	if err := f.pool.QueryRow(context.Background(), `SELECT published_at IS NOT NULL FROM outbox WHERE event_id=$1`, eventID).Scan(&published); err != nil || published {
		t.Fatalf("event marked despite exit before mark: published=%v err=%v", published, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := f.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(f.outputURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 2})
	if err != nil || len(output.Messages) != 1 {
		t.Fatalf("publish not actually delivered: %v messages=%v", err, output)
	}
	var envelope struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal([]byte(aws.ToString(output.Messages[0].Body)), &envelope); err != nil || envelope.EventID != eventID {
		t.Fatalf("output identity differs: %v id=%s", err, envelope.EventID)
	}
	f.config.InstanceID += "-replacement"
	f.engine(t)
	eventually(t, 15*time.Second, func() bool {
		var count int
		err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&count)
		return err == nil && count == 0
	})
	var attempts, different int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*),count(*) FILTER(WHERE payload<>$2) FROM recovery_observation WHERE operation='publish' AND identity=$1`, eventID, firstPayload).Scan(&attempts, &different); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || different != 0 {
		t.Fatalf("republish not stable: accepted sends=%d changed payloads=%d", attempts, different)
	}
	// FIFO may suppress the second visible delivery inside its five-minute
	// deduplication window. Two successful SendMessage calls are recorded above;
	// the test does not mistake queue deduplication for outbox exactly-once.
}

func TestRecoveryMalformedMessageNativeDLQ(t *testing.T) {
	f := newFixture(t)
	f.broker.fastRedrive = true
	f.send(t, `{"unexpected":"poison"}`, "poison")
	f.engine(t)
	eventually(t, 15*time.Second, func() bool {
		depths, err := f.broker.DLQDepths(context.Background())
		return err == nil && depths["input"] == 1
	})
	var received, maxCount int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*),COALESCE(max(receive_count),0) FROM recovery_observation WHERE operation='receive'`).Scan(&received, &maxCount); err != nil {
		t.Fatal(err)
	}
	if received < 5 || maxCount < 5 {
		t.Fatalf("DLQ without five proven deliveries: receives=%d max=%d", received, maxCount)
	}
	for _, table := range []string{"inbox", "wager_transaction", "wallet_ledger_entry", "outbox"} {
		var count int
		if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("poison left durable financial effects in %s count=%d error=%v", table, count, err)
		}
	}
	result, err := f.api.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{QueueUrl: aws.String(f.deadURL), WaitTimeSeconds: 2, MaxNumberOfMessages: 1})
	if err != nil || len(result.Messages) != 1 || !bytes.Contains([]byte(aws.ToString(result.Messages[0].Body)), []byte("poison")) {
		t.Fatalf("DLQ missing original message: %v", err)
	}
}

func TestRecoveryOutboxClaimFencingAndRetry(t *testing.T) {
	f := newFixture(t)
	f.wallet(t)
	ctx := context.Background()
	first, err := f.store.ClaimOutbox(ctx, "same-instance", 10, 20*time.Millisecond)
	if err != nil || len(first) != 2 {
		t.Fatalf("initial claim count=%d error=%v", len(first), err)
	}
	var replacement []application.OutboxRecord
	eventually(t, 3*time.Second, func() bool {
		claimed, claimErr := f.store.ClaimOutbox(ctx, "same-instance", 10, time.Minute)
		if claimErr != nil {
			return false
		}
		replacement = append(replacement, claimed...)
		return len(replacement) == 2
	})
	for _, old := range first {
		var current application.OutboxRecord
		for _, candidate := range replacement {
			if candidate.EventID == old.EventID {
				current = candidate
			}
		}
		if current.ClaimToken == "" || current.ClaimToken == old.ClaimToken {
			t.Fatal("reacquired claim did not receive new fencing token")
		}
		if err := f.store.MarkPublished(ctx, old.EventID, old.ClaimToken); application.Code(err) != "CLAIM_LOST" {
			t.Fatalf("stale publisher marked replacement: %v", err)
		}
		if err := f.store.RetryOutbox(ctx, old.EventID, old.ClaimToken, 0); application.Code(err) != "CLAIM_LOST" {
			t.Fatalf("stale publisher changed retry state: %v", err)
		}
		if err := f.store.RetryOutbox(ctx, current.EventID, current.ClaimToken, time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	var unpublished, retryCount, activeClaims int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE published_at IS NULL),sum(attempts),count(*) FILTER(WHERE claim_token IS NOT NULL) FROM outbox`).Scan(&unpublished, &retryCount, &activeClaims); err != nil {
		t.Fatal(err)
	}
	if unpublished != 2 || retryCount != 2 || activeClaims != 0 {
		t.Fatalf("retry lost event or failed release: pending=%d retries=%d claims=%d", unpublished, retryCount, activeClaims)
	}
	if premature, err := f.store.ClaimOutbox(ctx, "third-instance", 10, time.Minute); err != nil || len(premature) != 0 {
		t.Fatalf("backoff ignored: claimed=%d error=%v", len(premature), err)
	}
}

func assertFinancialState(t *testing.T, f *fixture, wallet string, balance, debits, inbox int64) {
	t.Helper()
	var actualBalance, actualDebits, actualInbox int64
	err := f.pool.QueryRow(context.Background(), `SELECT balance_minor,(SELECT count(*) FROM wallet_ledger_entry WHERE wallet_id=$1 AND direction='DEBIT'),(SELECT count(*) FROM inbox) FROM wallet WHERE id=$1`, wallet).Scan(&actualBalance, &actualDebits, &actualInbox)
	if err != nil || actualBalance != balance || actualDebits != debits || actualInbox != inbox {
		t.Fatalf("financial state balance=%d debits=%d inbox=%d err=%v", actualBalance, actualDebits, actualInbox, err)
	}
}
func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("condition not reached before deadline")
		case <-ticker.C:
		}
	}
}
func uuid(t *testing.T) string {
	t.Helper()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	id[6] = (id[6] & 15) | 64
	id[8] = (id[8] & 63) | 128
	s := hex.EncodeToString(id[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func rewrite(address, endpoint string) string {
	u, err := url.Parse(address)
	if err != nil {
		return address
	}
	base, err := url.Parse(endpoint)
	if err != nil {
		return address
	}
	u.Scheme, u.Host = base.Scheme, base.Host
	return u.String()
}
