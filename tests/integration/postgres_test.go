package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"jungle/internal/adapters/postgres"
	"jungle/internal/application"
	"jungle/internal/domain"
)

type fixture struct {
	pool  *pgxpool.Pool
	store *postgres.Store
	dsn   string
}

// openPool caps the pool explicitly. pgxpool defaults to one connection per
// host core, and these tests run in parallel: uncapped, a dozen of them would
// ask for more connections than PostgreSQL allows and fail as an exhausted
// server rather than as whatever they were written to prove.
func openPool(t *testing.T, dsn string, max int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = max
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func database(t *testing.T) fixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL required: real PostgreSQL 18 integration")
	}
	// Every test owns a private schema, so they are independent by construction.
	t.Parallel()
	ctx := context.Background()
	admin := openPool(t, dsn, 2)
	// Parallel tests start within the same nanosecond often enough that a clock
	// alone is not a unique name; the random suffix is what actually separates.
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("jungle_test_%d_%x", time.Now().UnixNano(), salt)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, e := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); e != nil {
			t.Error(e)
		}
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	dsn = u.String()
	migrate(t, dsn, "up")
	pool := openPool(t, dsn, 8)
	return fixture{pool, postgres.New(pool), dsn}
}
func migrate(t *testing.T, dsn, command string) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	withGoose(t, func() error {
		return goose.RunContext(context.Background(), command, db, filepath.Join(root, "migrations"))
	})
}

// goose keeps the dialect and its migration state in package-level variables,
// so it is not safe to drive from several tests at once. The dialect is set
// exactly once and the migration step is serialized: it costs about 40ms per
// test and removes the data race entirely, which is a trade every parallel
// suite here should take.
var (
	gooseOnce sync.Once
	gooseMu   sync.Mutex
)

func withGoose(t *testing.T, fn func() error) {
	t.Helper()
	gooseMu.Lock()
	defer gooseMu.Unlock()
	var setErr error
	gooseOnce.Do(func() { setErr = goose.SetDialect("postgres") })
	if setErr != nil {
		t.Fatal(setErr)
	}
	if err := fn(); err != nil {
		t.Fatal(err)
	}
}

func money(t *testing.T, value string) domain.Money {
	t.Helper()
	m, err := domain.ParseMoney(value, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func wallet(t *testing.T, f fixture, amount string) application.WalletView {
	t.Helper()
	var pid string
	if err := f.pool.QueryRow(context.Background(), "SELECT uuidv7()::text").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	w, err := f.store.CreateWallet(context.Background(), application.CreateWalletCommand{PlayerID: pid, InitialBalance: money(t, amount)})
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func command(t *testing.T, w application.WalletView, external, kind, amount string) application.Command {
	t.Helper()
	return application.Command{ProviderID: "provider-a", ExternalTransactionID: external, IdempotencyKey: "key:" + external, PlayerID: w.PlayerID, WalletID: w.ID, RoundID: "round-1", GameID: "game-1", Kind: kind, Money: money(t, amount)}
}
func process(t *testing.T, f fixture, c application.Command) application.Result {
	t.Helper()
	r, e := f.store.ProcessWager(context.Background(), c, nil)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func count(t *testing.T, f fixture, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := f.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func reconcile(t *testing.T, f fixture, w application.WalletView, expected string) {
	t.Helper()
	r, err := f.store.Reconcile(context.Background(), w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Consistent || r.StoredBalance.String() != expected {
		t.Fatalf("reconciliation %+v", r)
	}
}

func TestMigrationsAndDatabaseGuards(t *testing.T) {
	f := database(t)
	migrate(t, f.dsn, "reset")
	migrate(t, f.dsn, "up")
	w := wallet(t, f, "100.00")
	if w.ID[14] != '7' {
		t.Fatalf("internal ID not UUIDv7: %s", w.ID)
	}
	for _, query := range []string{
		`UPDATE wallet_ledger_entry SET amount_minor=amount_minor WHERE wallet_id=$1`,
		`DELETE FROM wallet_ledger_entry WHERE wallet_id=$1`,
		`UPDATE wallet SET balance_minor=-1 WHERE id=$1`,
		`UPDATE wager_transaction SET status='PENDING' WHERE wallet_id=$1`,
		`UPDATE outbox SET payload='{}'::jsonb WHERE aggregate_id=$1`,
	} {
		if _, err := f.pool.Exec(context.Background(), query, w.ID); err == nil {
			t.Errorf("guard accepted %s", query)
		}
	}
	if count(t, f, `SELECT count(*) FROM outbox WHERE event_id::text=payload->>'eventId'`) != 2 {
		t.Fatal("event IDs inconsistent")
	}
	if _, err := f.store.CreateWallet(context.Background(), application.CreateWalletCommand{PlayerID: w.PlayerID, InitialBalance: money(t, "1.00")}); application.Code(err) != "WALLET_ALREADY_EXISTS" {
		t.Fatalf("duplicate wallet: %v", err)
	}
	zero := wallet(t, f, "0.00")
	if count(t, f, `SELECT count(*) FROM wager_transaction WHERE wallet_id=$1`, zero.ID) != 0 {
		t.Fatal("zero opening generated wager")
	}
}

func TestReplayInboxAndOriginalResult(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	bet := command(t, w, "bet", "BET", "20.00")
	first := process(t, f, bet)
	if first.Status != "PROCESSED" || first.Balance.String() != "80.00" {
		t.Fatal(first)
	}
	process(t, f, command(t, w, "win", "WIN", "30.00"))
	replay := process(t, f, bet)
	if !replay.IdempotentReplay || replay.Balance.String() != "80.00" || replay.WalletVersion != 2 {
		t.Fatal(replay)
	}
	ih, _ := application.InboxHash("WagerTransactionRequested", time.Unix(123, 0), bet)
	inbox := &application.InboxMessage{ConsumerName: "wager", MessageID: "msg-1", PayloadHash: ih}
	replay, err := f.store.ProcessWager(context.Background(), bet, inbox)
	if err != nil || !replay.IdempotentReplay {
		t.Fatalf("SQS HTTP replay: %+v %v", replay, err)
	}
	if count(t, f, `SELECT count(*) FROM inbox WHERE completed_at IS NOT NULL`) != 1 {
		t.Fatal("replay inbox not committed")
	}
	replay, err = f.store.ProcessWager(context.Background(), bet, inbox)
	if err != nil || !replay.IdempotentReplay {
		t.Fatal(replay, err)
	}
	changed := bet
	changed.IdempotencyKey = "other"
	hash, _ := application.InboxHash("WagerTransactionRequested", time.Unix(123, 0), changed)
	inbox.PayloadHash = hash
	if _, err = f.store.ProcessWager(context.Background(), changed, inbox); application.Code(err) != "INBOX_MESSAGE_REUSED" {
		t.Fatalf("inbox divergence: %v", err)
	}
	if _, err = f.store.ProcessWager(context.Background(), changed, nil); application.Code(err) != "EXTERNAL_TRANSACTION_ID_REUSED" {
		t.Fatal(err)
	}
	changed = bet
	changed.Money = money(t, "21.00")
	if _, err = f.store.ProcessWager(context.Background(), changed, nil); application.Code(err) != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatal(err)
	}
	if _, err = f.store.GetTransaction(context.Background(), "provider-b", first.TransactionID); application.Code(err) != "NOT_FOUND" {
		t.Fatalf("provider leak: %v", err)
	}
	reconcile(t, f, w, "110.00")
}

func TestAtomicRollbackAndCommittedRejections(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	ctx := context.Background()
	_, err := f.pool.Exec(ctx, `CREATE FUNCTION fail_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$; CREATE TRIGGER fail_test BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION fail_test_outbox()`)
	if err != nil {
		t.Fatal(err)
	}
	bet := command(t, w, "atomic", "BET", "20.00")
	hash, _ := application.InboxHash("WagerTransactionRequested", time.Now(), bet)
	_, err = f.store.ProcessWager(ctx, bet, &application.InboxMessage{ConsumerName: "wager", MessageID: "atomic", PayloadHash: hash})
	if err == nil {
		t.Fatal("expected injected failure")
	}
	if _, err = f.pool.Exec(ctx, `DROP TRIGGER fail_test ON outbox;DROP FUNCTION fail_test_outbox()`); err != nil {
		t.Fatal(err)
	}
	if count(t, f, `SELECT count(*) FROM inbox`) != 0 || count(t, f, `SELECT count(*) FROM wager_transaction WHERE origin='EXTERNAL'`) != 0 {
		t.Fatal("partial transaction committed")
	}
	reconcile(t, f, w, "100.00")
	rejected := process(t, f, command(t, w, "too-much", "BET", "101.00"))
	if rejected.Status != "REJECTED" || rejected.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Fatal(rejected)
	}
	loss := process(t, f, command(t, w, "loss", "LOSS", "0.00"))
	if loss.WalletVersion != 1 {
		t.Fatal(loss)
	}
	invalid := process(t, f, command(t, w, "invalid", "BET", "0.00"))
	if invalid.FailureCode != "INVALID_AMOUNT_FOR_KIND" {
		t.Fatal(invalid)
	}
	mismatch := command(t, w, "currency", "LOSS", "0.00")
	mismatch.Money, _ = domain.ParseMoney("0", "USD")
	mismatchResult := process(t, f, mismatch)
	if mismatchResult.FailureCode != "CURRENCY_MISMATCH" || mismatchResult.Balance.Currency() != "BRL" {
		t.Fatal(mismatchResult)
	}
	if process(t, f, mismatch).Balance.Currency() != "BRL" {
		t.Fatal("replay currency corrupted")
	}
	if count(t, f, `SELECT count(*) FROM wallet_ledger_entry WHERE wallet_id=$1`, w.ID) != 1 {
		t.Fatal("rejection/loss created ledger")
	}
	reconcile(t, f, w, "100.00")
}

func TestPendingReferenceResolutionExpiryAndReversal(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	ctx := context.Background()
	refund := command(t, w, "refund", "REFUND", "30.00")
	refund.ReferenceExternalTransactionID = "bet"
	pending := process(t, f, refund)
	if pending.Status != "PENDING_REFERENCE" {
		t.Fatal(pending)
	}
	process(t, f, command(t, w, "bet", "BET", "30.00"))
	if _, err := f.pool.Exec(ctx, `UPDATE wager_transaction SET next_reference_attempt_at=now() WHERE status='PENDING_REFERENCE'`); err != nil {
		t.Fatal(err)
	}
	claims, err := f.store.ClaimReferences(ctx, "worker-a", 5, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatal(claims, err)
	}
	resolved, err := f.store.RetryReference(ctx, claims[0], 12, 24*time.Hour)
	if err != nil || resolved.Status != "PROCESSED" {
		t.Fatal(resolved, err)
	}
	second := command(t, w, "second", "ROLLBACK", "30.00")
	second.ReferenceExternalTransactionID = "bet"
	if r := process(t, f, second); r.FailureCode != "REFERENCE_ALREADY_REVERSED" {
		t.Fatal(r)
	}
	rollback := command(t, w, "rollback-refund", "ROLLBACK", "30.00")
	rollback.ReferenceExternalTransactionID = "refund"
	if r := process(t, f, rollback); r.Status != "PROCESSED" {
		t.Fatal(r)
	}
	if r := process(t, f, second); r.FailureCode != "REFERENCE_ALREADY_REVERSED" {
		t.Fatal(r)
	}
	missing := command(t, w, "missing", "ROLLBACK", "20.00")
	missing.ReferenceExternalTransactionID = "never"
	process(t, f, missing)
	if _, err = f.pool.Exec(ctx, `UPDATE wager_transaction SET next_reference_attempt_at=now() WHERE status='PENDING_REFERENCE'`); err != nil {
		t.Fatal(err)
	}
	claims, err = f.store.ClaimReferences(ctx, "worker-a", 5, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatal(claims, err)
	}
	expired, err := f.store.RetryReference(ctx, claims[0], 1, time.Hour)
	if err != nil || expired.FailureCode != "REFERENCE_NOT_FOUND" {
		t.Fatal(expired, err)
	}
	reconcile(t, f, w, "70.00")
}

func TestClaimFencingAndParallelPublishers(t *testing.T) {
	f := database(t)
	wallet(t, f, "100.00")
	ctx := context.Background()
	a, err := f.store.ClaimOutbox(ctx, "same-owner", 1, time.Minute)
	if err != nil || len(a) != 1 {
		t.Fatal(a, err)
	}
	b, err := f.store.ClaimOutbox(ctx, "second-owner", 5, time.Minute)
	if err != nil || len(b) != 1 || a[0].EventID == b[0].EventID {
		t.Fatal(a, b, err)
	}
	if _, err = f.pool.Exec(ctx, `UPDATE outbox SET claimed_until=now()-interval '1 second' WHERE event_id=$1`, a[0].EventID); err != nil {
		t.Fatal(err)
	}
	again, err := f.store.ClaimOutbox(ctx, "same-owner", 1, time.Minute)
	if err != nil || len(again) != 1 || again[0].ClaimToken == a[0].ClaimToken {
		t.Fatal(again, err)
	}
	if err = f.store.MarkPublished(ctx, a[0].EventID, a[0].ClaimToken); application.Code(err) != "CLAIM_LOST" {
		t.Fatal(err)
	}
	if err = f.store.RetryOutbox(ctx, a[0].EventID, a[0].ClaimToken, 0); application.Code(err) != "CLAIM_LOST" {
		t.Fatal(err)
	}
	if !bytes.Equal(again[0].Payload, a[0].Payload) {
		t.Fatal("republication changed payload")
	}
	if err = f.store.MarkPublished(ctx, again[0].EventID, again[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
}

func TestIndependentWalletDoesNotWaitForLockedWallet(t *testing.T) {
	f := database(t)
	a := wallet(t, f, "100.00")
	b := wallet(t, f, "100.00")
	tx, err := f.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(context.Background(), `SELECT id FROM wallet WHERE id=$1 FOR UPDATE`, a.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := f.store.ProcessWager(ctx, command(t, b, "parallel", "BET", "10.00"), nil)
	if err != nil || result.Status != "PROCESSED" {
		t.Fatalf("independent wallet blocked: %+v %v", result, err)
	}
}

type childJob struct {
	Command application.Command
	Count   int
}
type childResult struct {
	Result application.Result
	Error  string
}

// A helper invocation is a genuinely separate process and pool. Stdin is a
// deterministic start barrier; no shared-memory lock coordinates production work.
func TestPostgresProcessHelper(t *testing.T) {
	value := os.Getenv("JUNGLE_PROCESS_JOB")
	if value == "" {
		t.Skip("helper process")
	}
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	var job childJob
	if err = json.Unmarshal(raw, &job); err != nil {
		t.Fatal(err)
	}
	// Money intentionally has no transport decoder: construct it through the
	// external parser, exactly as production HTTP and SQS adapters do.
	var wire struct {
		Command struct {
			Money struct{ Amount, Currency string }
		}
	}
	if err = json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	job.Command.Money, err = domain.ParseMoney(wire.Command.Money.Amount, wire.Command.Money.Currency)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := postgres.New(pool)
	fmt.Println("READY")
	if _, err = bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	results := make([]childResult, job.Count)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, e := store.ProcessWager(context.Background(), job.Command, nil)
			results[i].Result = r
			if e != nil {
				results[i].Error = e.Error()
				results[i].Result.Balance, _ = domain.Zero("BRL")
			}
		}(i)
	}
	wg.Wait()
	data, err := json.Marshal(results)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("RESULT " + string(data))
}
func processes(t *testing.T, f fixture, jobs ...childJob) []childResult {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	type child struct {
		cmd     *exec.Cmd
		stdin   io.WriteCloser
		scanner *bufio.Scanner
		stderr  bytes.Buffer
	}
	children := make([]*child, 0, len(jobs))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, job := range jobs {
		raw, e := json.Marshal(job)
		if e != nil {
			t.Fatal(e)
		}
		c := &child{}
		c.cmd = exec.CommandContext(ctx, binary, "-test.run=^TestPostgresProcessHelper$")
		c.cmd.Env = append(os.Environ(), "TEST_DATABASE_URL="+f.dsn, "JUNGLE_PROCESS_JOB="+base64.StdEncoding.EncodeToString(raw))
		c.stdin, e = c.cmd.StdinPipe()
		if e != nil {
			t.Fatal(e)
		}
		stdout, e := c.cmd.StdoutPipe()
		if e != nil {
			t.Fatal(e)
		}
		c.scanner = bufio.NewScanner(stdout)
		c.scanner.Buffer(make([]byte, 4096), 1<<20)
		c.cmd.Stderr = &c.stderr
		if e = c.cmd.Start(); e != nil {
			t.Fatal(e)
		}
		children = append(children, c)
		if !c.scanner.Scan() || c.scanner.Text() != "READY" {
			t.Fatalf("child not ready: %s", c.stderr.String())
		}
	}
	for _, c := range children {
		if _, err = io.WriteString(c.stdin, "GO\n"); err != nil {
			t.Fatal(err)
		}
		c.stdin.Close()
	}
	var all []childResult
	for _, c := range children {
		var output strings.Builder
		for c.scanner.Scan() {
			line := c.scanner.Text()
			output.WriteString(line + "\n")
			if strings.HasPrefix(line, "RESULT ") {
				var results []childResult
				if err = json.Unmarshal([]byte(strings.TrimPrefix(line, "RESULT ")), &results); err != nil {
					t.Fatal(err)
				}
				all = append(all, results...)
			}
		}
		if err = c.cmd.Wait(); err != nil {
			t.Fatalf("child failed %v: %s\n%s", err, c.stderr.String(), output.String())
		}
	}
	for _, r := range all {
		if r.Error != "" {
			t.Fatal(r.Error)
		}
	}
	return all
}
func TestThreeProcessesFiftyDuplicatesAndOverspend(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	c := command(t, w, "duplicate", "BET", "10.00")
	results := processes(t, f, childJob{c, 17}, childJob{c, 17}, childJob{c, 16})
	if len(results) != 50 {
		t.Fatal(len(results))
	}
	originals := 0
	for _, r := range results {
		if !r.Result.IdempotentReplay {
			originals++
		}
	}
	if originals != 1 {
		t.Fatalf("original operations=%d", originals)
	}
	if count(t, f, `SELECT count(*) FROM wallet_ledger_entry WHERE wallet_id=$1 AND direction='DEBIT'`, w.ID) != 1 {
		t.Fatal("duplicate debit")
	}
	reconcile(t, f, w, "90.00")
	other := wallet(t, f, "100.00")
	first := command(t, other, "eighty-a", "BET", "80.00")
	second := command(t, other, "eighty-b", "BET", "80.00")
	results = processes(t, f, childJob{first, 1}, childJob{second, 1}, childJob{first, 1})
	processed, rejected := 0, 0
	for _, r := range results {
		if r.Result.IdempotentReplay {
			continue
		}
		if r.Result.Status == "PROCESSED" {
			processed++
		}
		if r.Result.FailureCode == "INSUFFICIENT_FUNDS" {
			rejected++
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed=%d rejected=%d results=%+v", processed, rejected, results)
	}
	if count(t, f, `SELECT count(*) FROM wallet_ledger_entry WHERE wallet_id=$1 AND direction='DEBIT'`, other.ID) != 1 {
		t.Fatal("overspend debit count")
	}
	reconcile(t, f, other, "20.00")
}
