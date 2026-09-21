package observability

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"jungle/internal/application"
)

// Prometheus panics when a label value count does not match the declaration, so
// a mismatch is a crash on the request path. Guarding it with a recover in the
// metrics middleware would hide the bug in production; the arity here is fixed,
// so the right guard is a test that fails the build instead.
func TestEveryObservationMatchesItsLabelArity(t *testing.T) {
	m := New()
	m.Observe("POST", "/v1/wagering/transactions", 201, 3*time.Millisecond)
	m.Observe("BREW", "/v1/wagering/transactions", 418, time.Millisecond)
	m.ObserveWager("http", "BET", "PROCESSED", 2*time.Millisecond)
	m.ObserveWager("sqs", "ROLLBACK", "REJECTED", time.Second)
	// Latency per route: the request counter always carried the route, the
	// duration never did, so nobody could tell which route was slow.
	if got := testutil.CollectAndCount(m.RouteDuration); got != 2 {
		t.Fatalf("route duration series = %d, want 2", got)
	}
	m.ObserveResult("http", "BET", "PROCESSED", "")
	m.ObserveResult("http", "BET", "REJECTED", "INSUFFICIENT_FUNDS")
	m.Duplicates.WithLabelValues("sqs").Inc()
	m.Retries.WithLabelValues("outbox", "publish").Inc()
	m.Conflicts.WithLabelValues("postgres").Inc()
	m.OutboxPublished.WithLabelValues("WagerTransactionProcessed", "published").Inc()
	m.Messages.WithLabelValues("input", "acknowledged").Inc()
	m.ReferenceAttempts.WithLabelValues("resolved").Inc()
	m.Reconciliations.WithLabelValues("true").Inc()
	tracer := m.SQLTracer()
	tracer.duration.WithLabelValues("GetWalletForUpdate").Observe(time.Millisecond.Seconds())
	tracer.queries.WithLabelValues("select:wallet").Observe(time.Millisecond.Seconds())
	m.BuildInfo("test", "instance-1")

	// A rejection is only actionable when its reason is a dimension: a player
	// short on funds and a broken provider integration must not share a series.
	if got := testutil.CollectAndCount(m.Results); got != 2 {
		t.Fatalf("result series = %d, want 2 (one per failure story)", got)
	}

	if got := testutil.CollectAndCount(m.WagerDuration); got != 2 {
		t.Fatalf("wager duration series = %d, want 2", got)
	}
	// An unknown method must collapse rather than open a new series per verb.
	if got := testutil.CollectAndCount(m.Requests); got != 2 {
		t.Fatalf("request series = %d, want 2", got)
	}
}

// The whole point of the histogram is answering "how long does a BET take", so
// the buckets have to resolve where this engine actually lives. The default
// Prometheus buckets start at 5ms and put a 2ms wager in the first bucket with
// everything else.
func TestWagerBucketsResolveBothLatencyRegimes(t *testing.T) {
	var belowFive, aboveHundred int
	for _, b := range financialBuckets {
		if b < 0.005 {
			belowFive++
		}
		if b >= 0.1 {
			aboveHundred++
		}
	}
	if belowFive < 3 {
		t.Fatalf("only %d bucket(s) below 5ms: an uncontended wager settles near 2ms and would be unresolvable", belowFive)
	}
	if aboveHundred < 3 {
		t.Fatalf("only %d bucket(s) at or above 100ms: a wager queued on a contended wallet takes hundreds of ms", aboveHundred)
	}
}

func TestWagerOutcomeKeepsItsSetSmallAndMeaningful(t *testing.T) {
	for _, c := range []struct {
		name   string
		result application.Result
		err    error
		want   string
	}{
		{"processed", application.Result{Status: "PROCESSED"}, nil, "PROCESSED"},
		{"rejected", application.Result{Status: "REJECTED"}, nil, "REJECTED"},
		{"pending", application.Result{Status: "PENDING_REFERENCE"}, nil, "PENDING_REFERENCE"},
		// A replay skips the financial work, so counting it as PROCESSED would
		// drag the percentile of real work downwards.
		{"replay", application.Result{Status: "PROCESSED", IdempotentReplay: true}, nil, "REPLAY"},
		{"failure", application.Result{}, &application.Error{Code: "IDEMPOTENCY_KEY_REUSED"}, "ERROR"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := WagerOutcome(c.result, c.err); got != c.want {
				t.Fatalf("outcome = %q, want %q", got, c.want)
			}
		})
	}
}

// The statement name is what makes a slow wallet lock distinguishable from a
// slow wager lock. It must also stay bounded: SQL text as a label would not.
// A latency label derived from SQL text is a cardinality incident waiting to
// happen unless the result set is closed. The table has to be one this schema
// owns, so an unfamiliar statement collapses instead of minting a series.
func TestStatementNameStaysWithinTheSchema(t *testing.T) {
	for _, c := range []struct{ sql, want string }{
		{"-- name: GetWalletForUpdate :one\nSELECT 1 FOR UPDATE", "GetWalletForUpdate"},
		{"SELECT id FROM wallet WHERE id=$1", "select:wallet"},
		{"INSERT INTO wallet_ledger_entry(wallet_id) VALUES($1)", "insert:wallet_ledger_entry"},
		{"UPDATE outbox SET published_at=now()", "update:outbox"},
		{"SELECT count(*) FROM inbox", "select:inbox"},
		// Neither a known table nor a known verb may invent a label.
		{"SELECT 1", "select:other"},
		{"SELECT * FROM pg_stat_activity", "select:other"},
		{"VACUUM ANALYZE", "other:other"},
		// Commit is where the deferred integrity triggers run, so it is worth
		// naming rather than leaving in the unrecognised bucket.
		{"commit", "tx:commit"},
		{"begin", "tx:begin"},
		{"rollback to savepoint s1", "tx:rollback"},
		{"SELECT * FROM " + strings.Repeat("z", 300), "select:other"},
	} {
		if got := statementName(c.sql); got != c.want {
			t.Errorf("statementName(%.40q) = %q, want %q", c.sql, got, c.want)
		}
	}
}

func TestStatusClassCollapsesCodes(t *testing.T) {
	for code, want := range map[int]string{201: "2xx", 200: "2xx", 302: "3xx", 404: "4xx", 422: "4xx", 503: "5xx"} {
		if got := statusClass(code); got != want {
			t.Fatalf("statusClass(%d) = %q, want %q", code, got, want)
		}
	}
}

func TestQueryNameIsExtractedAndBounded(t *testing.T) {
	for _, c := range []struct{ sql, want string }{
		{"-- name: GetWalletForUpdate :one\nSELECT 1 FOR UPDATE", "GetWalletForUpdate"},
		{"SELECT " + strings.Repeat("x", 200) + " FOR UPDATE", "adhoc"},
		{"-- name: " + strings.Repeat("N", 100) + " :one", "adhoc"},
		{"-- name:\n", "adhoc"},
	} {
		if got := queryName(c.sql); got != c.want {
			t.Fatalf("queryName(%.30q) = %q, want %q", c.sql, got, c.want)
		}
	}
}
