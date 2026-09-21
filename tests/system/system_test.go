//go:build integration

// Package system tests real process, PostgreSQL, Keycloak and SQS boundaries.
// Run only against disposable local infrastructure; each run owns its schema,
// FIFO queues and temporary Keycloak client and removes exactly those resources.
package system

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

type amount struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}
type wallet struct {
	ID       string `json:"id"`
	PlayerID string `json:"playerId"`
	Balance  amount `json:"balance"`
	Version  int64  `json:"version"`
}
type result struct {
	TransactionID    string `json:"transactionId"`
	Status           string `json:"status"`
	Balance          amount `json:"balance"`
	WalletVersion    int64  `json:"walletVersion"`
	FailureCode      string `json:"failureCode"`
	IdempotentReplay bool   `json:"idempotentReplay"`
}
type command struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          amount `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}
type response struct {
	status int
	body   []byte
	err    error
}
type process struct {
	cmd              *exec.Cmd
	address, logPath string
	done             chan error
	running          bool
}
type suite struct {
	t                                                  *testing.T
	root, dir, suffix, schema, dsn, keycloak, endpoint string
	pool, admin                                        *pgxpool.Pool
	broker                                             *sqs.Client
	queues                                             map[string]string
	queueNames                                         map[string]string
	apps                                               []*process
	tokens                                             map[string]string
	client                                             *http.Client
}

func TestDistributedSystem(t *testing.T) {
	if os.Getenv("SYSTEM_TEST") != "1" {
		t.Skip("set SYSTEM_TEST=1 and TEST_DATABASE_URL; requires real local PostgreSQL18, Keycloak and LocalStack")
	}
	s := newSuite(t)
	var replayCommand command
	var replayResult result
	t.Run("fifty_duplicates_across_three_processes", func(t *testing.T) {
		w := s.openWallet(t, "100.00")
		c := s.command(w, "duplicate", "BET", "1.00")
		responses := s.concurrent(c, "same-key", 50)
		created := 0
		id := ""
		for _, r := range responses {
			if r.err != nil || (r.status != 201 && r.status != 200) {
				t.Fatalf("duplicate status=%d body=%s err=%v", r.status, r.body, r.err)
			}
			var got result
			decode(t, r.body, &got)
			if r.status == 201 {
				created++
			}
			if id == "" {
				id = got.TransactionID
			}
			if got.TransactionID != id || got.Balance.Amount != "99.00" || got.WalletVersion != 2 {
				t.Fatalf("replay changed original result: %+v", got)
			}
		}
		if created != 1 {
			t.Fatalf("created=%d, want1", created)
		}
		s.assertLedger(t, w, "99.00", 1)
		replayCommand = c
		replayResult = result{TransactionID: id, Balance: amount{"99.00", "BRL"}, WalletVersion: 2}
	})
	t.Run("100_minus_80_minus_80", func(t *testing.T) {
		w := s.openWallet(t, "100.00")
		commands := []command{s.command(w, "bet-a", "BET", "80.00"), s.command(w, "bet-b", "BET", "80.00")}
		start := make(chan struct{})
		responses := make(chan response, 2)
		for i, c := range commands {
			go func() {
				<-start
				responses <- s.request(i, http.MethodPost, "/wagering/transactions", s.tokens["a"], c.ExternalTransactionID, c)
			}()
		}
		close(start)
		processed, rejected := 0, 0
		for range 2 {
			r := <-responses
			if r.err != nil {
				t.Fatal(r.err)
			}
			var got result
			decode(t, r.body, &got)
			switch r.status {
			case 201:
				processed++
			case 422:
				rejected++
				if got.FailureCode != "INSUFFICIENT_FUNDS" {
					t.Fatal(got)
				}
			default:
				t.Fatalf("status=%d body=%s", r.status, r.body)
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("processed=%d rejected=%d", processed, rejected)
		}
		for _, c := range commands {
			r := s.request(2, http.MethodPost, "/wagering/transactions", s.tokens["a"], c.ExternalTransactionID, c)
			requireStatus(t, r, 200)
		}
		s.assertLedger(t, w, "20.00", 1)
	})
	t.Run("real_oidc_scope_and_provider_isolation", func(t *testing.T) {
		w := s.openWallet(t, "100.00")
		c := s.command(w, "auth", "BET", "10.00")
		for _, token := range []string{"", "not-a-jwt", forge(s.tokens["a"])} {
			requireStatus(t, s.request(0, http.MethodPost, "/wagering/transactions", token, "auth-key", c), 401)
		}
		requireStatus(t, s.request(0, http.MethodPost, "/wallets", s.tokens["a"], "", map[string]any{"playerId": w.PlayerID, "initialBalance": amount{"0.00", "BRL"}}), 403)
		requireStatus(t, s.request(0, http.MethodPost, "/wagering/transactions", s.tokens["internal"], "auth-key", c), 403)
		requireStatus(t, s.request(0, http.MethodPost, "/wagering/transactions", s.tokens["b"], "auth-key", c), 403)
		s.assertLedger(t, w, "100.00", 0)
		r := s.request(0, http.MethodPost, "/wagering/transactions", s.tokens["a"], "auth-key", c)
		requireStatus(t, r, 201)
		var got result
		decode(t, r.body, &got)
		requireStatus(t, s.request(1, http.MethodGet, "/wagering/transactions/"+got.TransactionID, s.tokens["b"], "", nil), 404)
		requireStatus(t, s.request(1, http.MethodGet, "/providers/provider-a/wagering/transactions/"+c.ExternalTransactionID, s.tokens["b"], "", nil), 403)
		requireStatus(t, s.request(2, http.MethodPost, "/wagering/transactions", s.tokens["b"], "auth-key", c), 403)
		s.assertLedger(t, w, "90.00", 1)
	})
	t.Run("genuine_expired_idp_token", func(t *testing.T) {
		token, expires := s.shortToken(t)
		// Scope is intentionally valid; this first call proves the IdP signature,
		// audience and provider before testing the exact same expired bytes.
		requireStatus(t, s.request(0, http.MethodGet, "/wagering/transactions/"+replayResult.TransactionID, token, "", nil), 200)
		delay := time.Until(time.Unix(expires, 0).Add(6 * time.Second))
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			<-timer.C
		}
		requireStatus(t, s.request(0, http.MethodGet, "/wagering/transactions/"+replayResult.TransactionID, token, "", nil), 401)
	})
	t.Run("http_sqs_replay_and_actual_duplicate_receipts", func(t *testing.T) {
		messageID := "cross-" + s.suffix
		s.send(t, replayCommand, "same-key", messageID, "transport-one")
		s.send(t, replayCommand, "same-key", messageID, "transport-two")
		// Different FIFO deduplication IDs intentionally deliver both copies. A
		// persisted inbox plus two successful SQS counters proves consumer receipts.
		eventually(t, 25*time.Second, func() bool { return s.counter(`jungle_wager_results_total`, `transport="sqs"`) >= 2 })
		var count int
		var completed bool
		if err := s.pool.QueryRow(context.Background(), `SELECT count(*),bool_and(completed_at IS NOT NULL) FROM inbox WHERE message_id=$1`, messageID).Scan(&count, &completed); err != nil {
			t.Fatal(err)
		}
		if count != 1 || !completed {
			t.Fatalf("inbox count=%d completed=%t", count, completed)
		}
		w := wallet{ID: replayCommand.WalletID, PlayerID: replayCommand.PlayerID}
		s.assertLedger(t, w, "99.00", 1)
	})
	t.Run("abrupt_process_death_restart_and_reference_recovery", func(t *testing.T) {
		w := s.openWallet(t, "100.00")
		bet := s.command(w, "late-bet", "BET", "20.00")
		refund := s.command(w, "early-refund", "REFUND", "20.00")
		refund.ReferenceExternalTransactionID = bet.ExternalTransactionID
		r := s.request(0, http.MethodPost, "/wagering/transactions", s.tokens["a"], "refund-key", refund)
		requireStatus(t, r, 202)
		var pending result
		decode(t, r.body, &pending)
		// No app memory survives: one process is SIGKILLed; both peers drain.
		s.stop(0, true)
		s.stop(1, false)
		s.stop(2, false)
		for i := range s.apps {
			s.start(i)
		}
		r = s.request(1, http.MethodPost, "/wagering/transactions", s.tokens["a"], "same-key", replayCommand)
		requireStatus(t, r, 200)
		var replay result
		decode(t, r.body, &replay)
		if replay.TransactionID != replayResult.TransactionID || replay.Balance != replayResult.Balance || replay.WalletVersion != replayResult.WalletVersion {
			t.Fatalf("restart lost replay: %+v", replay)
		}
		requireStatus(t, s.request(2, http.MethodPost, "/wagering/transactions", s.tokens["a"], "late-bet-key", bet), 201)
		eventually(t, 30*time.Second, func() bool {
			r := s.request(1, http.MethodGet, "/wagering/transactions/"+pending.TransactionID, s.tokens["a"], "", nil)
			var got result
			return r.err == nil && r.status == 200 && json.Unmarshal(r.body, &got) == nil && got.Status == "PROCESSED"
		})
		s.assertLedger(t, w, "100.00", 1)
		var credits int
		if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM wallet_ledger_entry WHERE wallet_id=$1 AND direction='CREDIT'`, w.ID).Scan(&credits); err != nil || credits != 2 {
			t.Fatalf("opening+refund credits=%d err=%v", credits, err)
		}
		requireStatus(t, s.request(0, http.MethodPost, "/wagering/transactions", s.tokens["a"], "refund-key", refund), 200)
	})
	t.Run("every_committed_event_reaches_real_sqs", func(t *testing.T) { s.assertPublished(t) })
}

func newSuite(t *testing.T) *suite {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL must identify a disposable local PostgreSQL18 database")
	}
	var random [6]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	s := &suite{t: t, root: filepath.Clean(filepath.Join(filepath.Dir(file), "../..")), dir: t.TempDir(), suffix: hex.EncodeToString(random[:]), keycloak: env("TEST_KEYCLOAK_URL", "http://localhost:8081"), endpoint: env("TEST_SQS_ENDPOINT", "http://localhost:4566"), queues: map[string]string{}, queueNames: map[string]string{}, tokens: map[string]string{}, client: &http.Client{Timeout: 10 * time.Second}}
	s.schema = "jungle_system_" + s.suffix
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var err error
	s.admin, err = pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{s.schema}.Sanitize()); err != nil {
		s.admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(s.cleanup)
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", s.schema)
	u.RawQuery = q.Encode()
	s.dsn = u.String()
	s.pool, err = pgxpool.New(ctx, s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = goose.SetDialect("postgres"); err == nil {
		err = goose.UpContext(ctx, db, filepath.Join(s.root, "migrations"))
	}
	_ = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	s.broker = sqs.New(sqs.Options{Region: "us-east-1", BaseEndpoint: aws.String(s.endpoint), Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")})
	for _, kind := range []string{"dlq", "input", "output"} {
		name := "jungle-system-" + kind + "-" + s.suffix + ".fifo"
		created, e := s.broker.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "20"}})
		if e != nil {
			t.Fatal(e)
		}
		s.queueNames[kind] = name
		s.queues[kind] = s.rewriteURL(aws.ToString(created.QueueUrl))
	}
	attributes, err := s.broker.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(s.queues["dlq"]), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attributes.Attributes["QueueArn"], "maxReceiveCount": "5"})
	if _, err = s.broker.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(s.queues["input"]), Attributes: map[string]string{"RedrivePolicy": string(redrive)}}); err != nil {
		t.Fatal(err)
	}
	s.tokens["a"] = s.token(t, "provider-a-client", "provider-a-local-only")
	s.tokens["b"] = s.token(t, "provider-b-client", "provider-b-local-only")
	s.tokens["internal"] = s.token(t, "wallet-internal-client", "wallet-internal-local-only")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-race", "-o", filepath.Join(s.dir, "api"), "./cmd/api")
	build.Dir = s.root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build application: %v\n%s", err, output)
	}
	for i := range 3 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		s.apps = append(s.apps, &process{address: address})
		s.start(i)
	}
	t.Logf("three independent processes ready; schema=%s queues=%s", s.schema, s.suffix)
	return s
}

func (s *suite) start(index int) {
	s.t.Helper()
	p := s.apps[index]
	p.logPath = filepath.Join(s.dir, fmt.Sprintf("app-%d.log", index))
	log, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		s.t.Fatal(err)
	}
	p.cmd = exec.Command(filepath.Join(s.dir, "api"))
	p.cmd.Dir = s.root
	p.cmd.Env = overrideEnv(os.Environ(), map[string]string{"DATABASE_URL": s.dsn, "HTTP_ADDR": p.address, "INSTANCE_ID": fmt.Sprintf("system-%s-%d", s.suffix, index), "OIDC_ISSUER": s.keycloak + "/realms/jungle", "OIDC_FETCH_ISSUER": s.keycloak + "/realms/jungle", "OIDC_AUDIENCE": "jungle-api", "OIDC_ALLOW_INSECURE_HTTP": "true", "AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_EC2_METADATA_DISABLED": "true", "SQS_ENDPOINT": s.endpoint, "SQS_INPUT_QUEUE": s.queueNames["input"], "SQS_OUTPUT_QUEUE": s.queueNames["output"], "SQS_INPUT_DLQ": s.queueNames["dlq"], "PROCESS_TIMEOUT": "5s", "SQS_VISIBILITY_TIMEOUT": "20s", "SHUTDOWN_TIMEOUT": "10s", "CLAIM_LEASE": "16s", "POLL_INTERVAL": "100ms", "REFERENCE_TTL": "1h", "REFERENCE_MAX_ATTEMPTS": "12", "WORKER_CONCURRENCY": "4"})
	p.cmd.Stdout, p.cmd.Stderr = log, log
	p.done = make(chan error, 1)
	if err = p.cmd.Start(); err != nil {
		_ = log.Close()
		s.t.Fatal(err)
	}
	p.running = true
	go func() { err := p.cmd.Wait(); _ = log.Close(); p.done <- err }()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-p.done:
			p.running = false
			data, _ := os.ReadFile(p.logPath)
			s.t.Fatalf("app%d startup %v\n%s", index, err, data)
		default:
		}
		r := s.request(index, http.MethodGet, "/health/ready", "", "", nil)
		if r.err == nil && r.status == 200 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	data, _ := os.ReadFile(p.logPath)
	s.t.Fatalf("app%d readiness timeout\n%s", index, data)
}

func (s *suite) stop(index int, kill bool) {
	p := s.apps[index]
	if !p.running {
		return
	}
	if kill {
		_ = p.cmd.Process.Kill()
	} else {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case err := <-p.done:
		if !kill && err != nil {
			s.t.Errorf("app%d graceful exit: %v", index, err)
		}
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		s.t.Errorf("app%d failed to drain within deadline", index)
	}
	p.running = false
}

func (s *suite) cleanup() {
	for i := range s.apps {
		s.stop(i, false)
	}
	if s.t.Failed() {
		for i, p := range s.apps {
			data, _ := os.ReadFile(p.logPath)
			if len(data) > 12000 {
				data = data[len(data)-12000:]
			}
			s.t.Logf("app%d final logs:\n%s", i, data)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if s.broker != nil {
		for _, address := range s.queues {
			if _, err := s.broker.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(address)}); err != nil {
				s.t.Logf("queue cleanup: %v", err)
			}
		}
	}
	if s.pool != nil {
		s.pool.Close()
	}
	if s.admin != nil {
		if _, err := s.admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{s.schema}.Sanitize()+" CASCADE"); err != nil {
			s.t.Logf("schema cleanup: %v", err)
		}
		s.admin.Close()
	}
}

func (s *suite) request(index int, method, path, token, key string, body any) response {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return response{err: err}
		}
	}
	req, err := http.NewRequest(method, "http://"+s.apps[index].address+path, bytes.NewReader(payload))
	if err != nil {
		return response{err: err}
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	r, err := s.client.Do(req)
	if err != nil {
		return response{err: err}
	}
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	return response{r.StatusCode, data, err}
}

func (s *suite) openWallet(t *testing.T, balance string) wallet {
	t.Helper()
	var player string
	if err := s.pool.QueryRow(context.Background(), "SELECT uuidv7()::text").Scan(&player); err != nil {
		t.Fatal(err)
	}
	r := s.request(0, http.MethodPost, "/wallets", s.tokens["internal"], "", map[string]any{"playerId": player, "initialBalance": amount{balance, "BRL"}})
	requireStatus(t, r, 201)
	var w wallet
	decode(t, r.body, &w)
	if w.ID == "" {
		t.Fatal("missing wallet identity")
	}
	return w
}
func (s *suite) command(w wallet, id, kind, value string) command {
	return command{ProviderID: "provider-a", ExternalTransactionID: id + "-" + w.ID, PlayerID: w.PlayerID, WalletID: w.ID, RoundID: "round-" + w.ID, GameID: "game", Kind: kind, Money: amount{value, "BRL"}}
}
func (s *suite) concurrent(c command, key string, count int) []response {
	start := make(chan struct{})
	responses := make(chan response, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			responses <- s.request(i%len(s.apps), http.MethodPost, "/wagering/transactions", s.tokens["a"], key, c)
		}()
	}
	close(start)
	wg.Wait()
	close(responses)
	all := make([]response, 0, count)
	for r := range responses {
		all = append(all, r)
	}
	return all
}
func (s *suite) assertLedger(t *testing.T, w wallet, balance string, debits int) {
	t.Helper()
	r := s.request(0, http.MethodGet, "/wallets/"+w.ID, s.tokens["internal"], "", nil)
	requireStatus(t, r, 200)
	var got wallet
	decode(t, r.body, &got)
	if got.Balance.Amount != balance {
		t.Fatalf("balance=%s want%s", got.Balance.Amount, balance)
	}
	var count int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM wallet_ledger_entry WHERE wallet_id=$1 AND direction='DEBIT'`, w.ID).Scan(&count); err != nil || count != debits {
		t.Fatalf("debits=%d want%d err=%v", count, debits, err)
	}
	r = s.request(1, http.MethodPost, "/wallets/"+w.ID+"/reconciliation", s.tokens["internal"], "", nil)
	requireStatus(t, r, 200)
	var rec struct {
		Consistent                                   bool
		StoredBalance, CalculatedBalance, Difference amount
	}
	decode(t, r.body, &rec)
	if !rec.Consistent || rec.StoredBalance.Amount != balance || rec.CalculatedBalance.Amount != balance || rec.Difference.Amount != "0.00" {
		t.Fatalf("reconciliation=%s", r.body)
	}
}

func (s *suite) send(t *testing.T, c command, key, messageID, dedup string) {
	t.Helper()
	data, _ := json.Marshal(c)
	var fields map[string]any
	decode(t, data, &fields)
	fields["idempotencyKey"] = key
	// The fixed occurrence is part of the inbox fingerprint and stays identical.
	body, _ := json.Marshal(map[string]any{"messageId": messageID, "type": "WagerTransactionRequested", "occurredAt": "2026-09-19T12:00:00Z", "data": fields})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.broker.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(s.queues["input"]), MessageGroupId: aws.String(c.WalletID), MessageDeduplicationId: aws.String(dedup), MessageBody: aws.String(string(body))})
	if err != nil {
		t.Fatal(err)
	}
}

func (s *suite) counter(name, label string) int64 {
	var sum int64
	for i := range s.apps {
		r := s.request(i, http.MethodGet, "/metrics", "", "", nil)
		if r.err != nil || r.status != 200 {
			continue
		}
		for _, line := range strings.Split(string(r.body), "\n") {
			if strings.HasPrefix(line, name+"{") && strings.Contains(line, label) {
				fields := strings.Fields(line)
				if len(fields) == 2 {
					n, _ := strconv.ParseInt(fields[1], 10, 64)
					sum += n
				}
			}
		}
	}
	return sum
}

func (s *suite) assertPublished(t *testing.T) {
	t.Helper()
	expected := map[string]bool{}
	rows, err := s.pool.Query(context.Background(), "SELECT event_id::text FROM outbox")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		expected[id] = false
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	received := 0
	for time.Now().Before(deadline) && received < len(expected) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		messages, err := s.broker.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(s.queues["output"]), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		for _, message := range messages.Messages {
			var event struct {
				EventID string
				Version int
			}
			decode(t, []byte(aws.ToString(message.Body)), &event)
			seen, exists := expected[event.EventID]
			if !exists || event.Version != 1 {
				t.Fatalf("unknown/incompatible event %s", aws.ToString(message.Body))
			}
			if !seen {
				expected[event.EventID] = true
				received++
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err = s.broker.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(s.queues["output"]), ReceiptHandle: message.ReceiptHandle})
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if received != len(expected) {
		t.Fatalf("published unique events=%d committed=%d", received, len(expected))
	}
	eventually(t, 5*time.Second, func() bool {
		var count int
		return s.pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&count) == nil && count == 0
	})
}

func (s *suite) token(t *testing.T, id, secret string) string {
	t.Helper()
	values := url.Values{"grant_type": {"client_credentials"}, "client_id": {id}, "client_secret": {secret}}
	r, err := s.client.PostForm(s.keycloak+"/realms/jungle/protocol/openid-connect/token", values)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err = json.NewDecoder(r.Body).Decode(&body); err != nil || r.StatusCode != 200 || body.AccessToken == "" {
		t.Fatalf("Keycloak client token: status%d err%v", r.StatusCode, err)
	}
	return body.AccessToken
}

func (s *suite) shortToken(t *testing.T) (string, int64) {
	t.Helper()
	r, err := s.client.PostForm(s.keycloak+"/realms/master/protocol/openid-connect/token", url.Values{"grant_type": {"password"}, "client_id": {"admin-cli"}, "username": {env("TEST_KEYCLOAK_ADMIN", "local-admin")}, "password": {env("TEST_KEYCLOAK_ADMIN_PASSWORD", "local-admin-only")}})
	if err != nil {
		t.Fatal(err)
	}
	var auth struct {
		AccessToken string `json:"access_token"`
	}
	err = json.NewDecoder(r.Body).Decode(&auth)
	_ = r.Body.Close()
	if err != nil || r.StatusCode != 200 || auth.AccessToken == "" {
		t.Fatalf("admin token: status%d err%v", r.StatusCode, err)
	}
	id, secret := "expiry-"+s.suffix, "test-only-"+s.suffix
	client := map[string]any{"clientId": id, "secret": secret, "enabled": true, "protocol": "openid-connect", "publicClient": false, "serviceAccountsEnabled": true, "standardFlowEnabled": false, "directAccessGrantsEnabled": false, "fullScopeAllowed": false, "attributes": map[string]string{"access.token.lifespan": "2"}, "defaultClientScopes": []string{"wager:read"}, "protocolMappers": []map[string]any{
		{"name": "provider", "protocol": "openid-connect", "protocolMapper": "oidc-hardcoded-claim-mapper", "config": map[string]string{"claim.name": "provider_id", "claim.value": "provider-a", "jsonType.label": "String", "access.token.claim": "true"}},
		{"name": "audience", "protocol": "openid-connect", "protocolMapper": "oidc-audience-mapper", "config": map[string]string{"included.custom.audience": "jungle-api", "access.token.claim": "true"}},
	}}
	body, _ := json.Marshal(client)
	req, _ := http.NewRequest(http.MethodPost, s.keycloak+"/admin/realms/jungle/clients", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+auth.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	r, err = s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Body.Close()
	if r.StatusCode != 201 {
		t.Fatalf("create short-lived client status%d", r.StatusCode)
	}
	location := r.Header.Get("Location")
	parsed, err := url.Parse(location)
	trusted, _ := url.Parse(s.keycloak)
	if err != nil || parsed.Host != trusted.Host || !strings.HasPrefix(parsed.Path, "/admin/realms/jungle/clients/") {
		t.Fatal("unexpected Keycloak client location")
	}
	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodDelete, location, nil)
		req.Header.Set("Authorization", "Bearer "+auth.AccessToken)
		r, err := s.client.Do(req)
		if err == nil {
			_ = r.Body.Close()
		}
		if err != nil || r.StatusCode != 204 {
			t.Errorf("remove short-lived client: %v", err)
		}
	})
	token := s.token(t, id, secret)
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatal("invalid token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	decode(t, payload, &claims)
	return token, claims.Exp
}

func (s *suite) rewriteURL(address string) string {
	u, err := url.Parse(address)
	if err != nil {
		s.t.Fatal(err)
	}
	endpoint, err := url.Parse(s.endpoint)
	if err != nil {
		s.t.Fatal(err)
	}
	u.Scheme, u.Host = endpoint.Scheme, endpoint.Host
	return u.String()
}
func forge(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "invalid"
	}
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var fields map[string]any
	_ = json.Unmarshal(payload, &fields)
	fields["provider_id"] = "provider-b"
	payload, _ = json.Marshal(fields)
	parts[1] = base64.RawURLEncoding.EncodeToString(payload)
	return strings.Join(parts, ".")
}
func overrideEnv(base []string, values map[string]string) []string {
	result := make([]string, 0, len(base)+len(values))
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		if _, replace := values[key]; !replace {
			result = append(result, entry)
		}
	}
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}
func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
func requireStatus(t *testing.T, r response, status int) {
	t.Helper()
	if r.err != nil || r.status != status {
		t.Fatalf("HTTP status=%d want%d body=%s err=%v", r.status, status, r.body, r.err)
	}
}
func decode(t *testing.T, data []byte, destination any) {
	t.Helper()
	if err := json.Unmarshal(data, destination); err != nil {
		t.Fatalf("decode response: %v body=%s", err, data)
	}
}
func eventually(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condition not satisfied within %s", timeout)
}
