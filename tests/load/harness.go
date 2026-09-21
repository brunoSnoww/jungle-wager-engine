//go:build load

// Package load drives the running service at a constant open-model rate and
// then asserts that the financial invariants survived the pressure. A rate
// number without that verdict is not publishable.
package load

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type settings struct {
	targets  []string
	rate     int
	duration time.Duration
	wallets  int
	keycloak string
	dsn      string
}

func loadSettings(t *testing.T) settings {
	t.Helper()
	s := settings{
		targets:  strings.Split(env("LOAD_TARGETS", "http://localhost:8080,http://localhost:8082,http://localhost:8083"), ","),
		rate:     number(t, "LOAD_RATE", 200),
		wallets:  number(t, "LOAD_WALLETS", 64),
		keycloak: env("TEST_KEYCLOAK_URL", "http://localhost:8081"),
		dsn:      os.Getenv("TEST_DATABASE_URL"),
	}
	d, err := time.ParseDuration(env("LOAD_DURATION", "20s"))
	if err != nil {
		t.Fatalf("LOAD_DURATION: %v", err)
	}
	s.duration = d
	if s.dsn == "" {
		t.Skip("TEST_DATABASE_URL required: load runs against a disposable local stack")
	}
	if s.rate < 1 || s.wallets < 1 || s.duration <= 0 {
		t.Fatalf("invalid load settings: %+v", s)
	}
	return s
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func number(t *testing.T, key string, fallback int) int {
	t.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	return v
}

type lab struct {
	t        *testing.T
	settings settings
	client   *http.Client
	pool     *pgxpool.Pool
	internal *credential
	provider *credential
	next     atomic.Uint64
	// run scopes every external identity to this execution. Without it a second
	// run reuses provider-a's external transaction IDs and the engine correctly
	// answers 409, which is right behaviour but destroys the measurement.
	run string
	// startedAt scopes outbox queries to this run.
	startedAt time.Time
}

// credential keeps a usable bearer token for runs longer than the realm's
// 300s access token lifespan, so a soak does not decay into 401s.
type credential struct {
	token    atomic.Pointer[string]
	failures atomic.Int32
}

func (c *credential) bearer() string { return "Bearer " + *c.token.Load() }

func newLab(t *testing.T) *lab {
	t.Helper()
	s := loadSettings(t)
	// The generator must not be the bottleneck: give it far more idle
	// connections than the offered rate can have in flight at once.
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 2048,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
	}
	t.Cleanup(transport.CloseIdleConnections)
	l := &lab{t: t, settings: s, client: &http.Client{Timeout: 30 * time.Second, Transport: transport}}
	l.run = strconv.FormatInt(time.Now().UnixNano(), 36)
	l.startedAt = time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	l.pool = pool
	// These are the committed local-dev realm values from deploy/keycloak, not
	// production credentials. TEST_KEYCLOAK_URL is operator-controlled and the
	// secret travels in a form body, so never point this harness at a shared
	// realm or run it behind an inspecting proxy.
	l.internal = l.credential("wallet-internal-client", "wallet-internal-local-only")
	l.provider = l.credential("provider-a-client", "provider-a-local-only")
	return l
}

func (l *lab) credential(clientID, secret string) *credential {
	l.t.Helper()
	c := &credential{}
	token, lifespan := l.fetchToken(clientID, secret)
	c.token.Store(&token)
	refresh := lifespan / 2
	if refresh < 30*time.Second {
		refresh = 30 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	var done sync.WaitGroup
	done.Add(1)
	// Cleanup joins the goroutine: cancelling alone would let a refresh in flight
	// outlive the test and hold a connection.
	l.t.Cleanup(func() { cancel(); done.Wait() })
	go func() {
		defer done.Done()
		ticker := time.NewTicker(refresh)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// A refresh failure must not abort the run from a goroutine, so it
				// is counted and asserted on the test goroutine instead. Silently
				// ignoring it would turn every later request into a 401 while the
				// profile still printed a throughput number.
				fresh, lifespan, err := l.token(clientID, secret)
				if err != nil {
					c.failures.Add(1)
					continue
				}
				c.token.Store(&fresh)
				if lifespan > 0 && lifespan/2 != refresh {
					refresh = lifespan / 2
					ticker.Reset(refresh)
				}
			}
		}
	}()
	return c
}

// assertTokensHeld fails a run whose credentials silently decayed, which would
// otherwise look like a clean result over a wall of 401s.
func (l *lab) assertTokensHeld() {
	l.t.Helper()
	if n := l.internal.failures.Load() + l.provider.failures.Load(); n != 0 {
		l.t.Fatalf("%d token refreshes failed: the run's authorization decayed", n)
	}
}

func (l *lab) fetchToken(clientID, secret string) (string, time.Duration) {
	l.t.Helper()
	token, lifespan, err := l.token(clientID, secret)
	if err != nil {
		l.t.Fatalf("keycloak %s: %v", clientID, err)
	}
	return token, lifespan
}

func (l *lab) token(clientID, secret string) (string, time.Duration, error) {
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}}
	resp, err := l.client.PostForm(l.settings.keycloak+"/realms/jungle/protocol/openid-connect/token", form)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("token status %d", resp.StatusCode)
	}
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", 0, err
	}
	if body.AccessToken == "" {
		return "", 0, fmt.Errorf("empty access token")
	}
	return body.AccessToken, time.Duration(body.ExpiresIn) * time.Second, nil
}

// target spreads offered load across every replica so the measurement is of
// the shared PostgreSQL boundary, not of one process.
func (l *lab) target(path string) string {
	t := l.settings.targets[int(l.next.Add(1))%len(l.settings.targets)]
	return strings.TrimRight(t, "/") + path
}

type wallet struct {
	ID       string `json:"id"`
	PlayerID string `json:"playerId"`
}

func (l *lab) openWallet(balance string) wallet {
	l.t.Helper()
	player := uuidv4(l.t)
	payload := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, player, balance)
	req, err := http.NewRequest(http.MethodPost, l.target("/wallets"), strings.NewReader(payload))
	if err != nil {
		l.t.Fatal(err)
	}
	req.Header.Set("Authorization", l.internal.bearer())
	req.Header.Set("Content-Type", "application/json")
	resp, err := l.client.Do(req)
	if err != nil {
		l.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		l.t.Fatalf("open wallet status %d: %s", resp.StatusCode, snippet(resp.Body))
	}
	var w wallet
	if err = json.NewDecoder(resp.Body).Decode(&w); err != nil {
		l.t.Fatal(err)
	}
	w.PlayerID = player
	return w
}

func uuidv4(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

type money struct {
	Amount string `json:"amount"`
}
type reconciliation struct {
	StoredBalance     money `json:"storedBalance"`
	CalculatedBalance money `json:"calculatedBalance"`
	Difference        money `json:"difference"`
	Consistent        bool  `json:"consistent"`
	CheckedEntries    int64 `json:"checkedEntries"`
}

func (l *lab) reconcile(id string) reconciliation {
	l.t.Helper()
	req, err := http.NewRequest(http.MethodPost, l.target("/wallets/"+id+"/reconciliation"), nil)
	if err != nil {
		l.t.Fatal(err)
	}
	req.Header.Set("Authorization", l.internal.bearer())
	resp, err := l.client.Do(req)
	if err != nil {
		l.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		l.t.Fatalf("reconciliation status %d for %s: %s", resp.StatusCode, id, snippet(resp.Body))
	}
	var r reconciliation
	if err = json.NewDecoder(resp.Body).Decode(&r); err != nil {
		l.t.Fatal(err)
	}
	return r
}

func (l *lab) assertConsistent(wallets ...wallet) {
	l.t.Helper()
	for _, w := range wallets {
		r := l.reconcile(w.ID)
		if !r.Consistent || r.Difference.Amount != "0.00" {
			// Errorf, not Fatalf: with 64 wallets the blast radius is the finding.
			l.t.Errorf("wallet %s diverged under load: stored=%s calculated=%s difference=%s",
				w.ID, r.StoredBalance.Amount, r.CalculatedBalance.Amount, r.Difference.Amount)
		}
	}
	if l.t.Failed() {
		l.t.FailNow()
	}
}

// snippet bounds a failure body so a setup error is diagnosable without dumping
// a response into the log.
func snippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 2048))
	return strings.TrimSpace(string(b))
}

// cents renders a minor-unit amount as the API's canonical two-decimal string.
func cents(v int) string { return fmt.Sprintf("%d.%02d", v/100, v%100) }

// awaitScalar polls a count toward want, so a commit still in flight when the
// generator stopped is not mistaken for a missing one.
func (l *lab) awaitScalar(want int64, within time.Duration, query string, args ...any) int64 {
	l.t.Helper()
	deadline := time.Now().Add(within)
	got := l.scalar(query, args...)
	for got < want && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		got = l.scalar(query, args...)
	}
	return got
}

// queryRow exposes a row for assertions that need several columns at once.
func (l *lab) queryRow(query string, args ...any) pgx.Row { //nolint:ireturn
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	l.t.Cleanup(cancel)
	return l.pool.QueryRow(ctx, query, args...)
}

// counter reads a Prometheus counter summed across every replica. The load
// report is required to state concurrency conflicts alongside throughput and
// latency, and conflicts are only visible from the service, never from the
// generator: a 409 is one attempt losing a race that another attempt won.
func (l *lab) counter(metric string) float64 {
	l.t.Helper()
	var total float64
	scraped := 0
	for _, base := range l.settings.targets {
		resp, err := l.client.Get(strings.TrimRight(base, "/") + "/metrics")
		if err != nil {
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			continue
		}
		scraped++
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.HasPrefix(line, metric) {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			// A bare prefix match would also sum jungle_references_pending when
			// asked for jungle_reference, and publish the inflated number.
			if rest := line[len(metric):]; rest != "" && rest[0] != '{' && rest[0] != ' ' {
				continue
			}
			if v, err := strconv.ParseFloat(fields[1], 64); err == nil {
				total += v
			}
		}
	}
	if scraped == 0 {
		// Returning 0 here would let a dead fleet be reported as a clean result.
		l.t.Fatalf("no replica served /metrics; %q could not be read", metric)
	}
	return total
}

func (l *lab) scalar(query string, args ...any) int64 {
	l.t.Helper()
	var n int64
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := l.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		l.t.Fatal(err)
	}
	return n
}

// awaitOutboxDrained proves the publisher kept up: every event committed under
// load eventually reaches the broker, with no permanent backlog. It also times
// the drain, because publisher throughput is a separate ceiling from ingest
// throughput and a load test that ignores it reports only half the system.
func (l *lab) awaitOutboxDrained(within time.Duration) {
	l.t.Helper()
	started := time.Now()
	// Scoped to this run: a leftover backlog from an earlier run would otherwise
	// be attributed to this one, and its drain time reported as this run's.
	backlog := l.scalar(`SELECT count(*) FROM outbox WHERE published_at IS NULL AND occurred_at>=$1`, l.startedAt)
	deadline := started.Add(within)
	pending := backlog
	for time.Now().Before(deadline) {
		if pending == 0 {
			elapsed := time.Since(started)
			// A small backlog means the publisher kept pace, and dividing it by
			// one poll tick would report a meaningless rate. Only a real backlog
			// measures publisher throughput.
			if backlog < 100 {
				l.t.Logf("[outbox] publisher kept pace: backlog was %d at end of attack", backlog)
				return
			}
			l.t.Logf("[outbox] drained backlog=%d in %s (%.0f events/s)", backlog, elapsed.Round(time.Millisecond), float64(backlog)/elapsed.Seconds())
			return
		}
		time.Sleep(250 * time.Millisecond)
		pending = l.scalar(`SELECT count(*) FROM outbox WHERE published_at IS NULL AND occurred_at>=$1`, l.startedAt)
	}
	l.t.Fatalf("outbox still holds %d of %d events unpublished after %s", pending, backlog, within)
}
