package observability

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"time"
)

type lockTimingKey struct{}
type SQLTracer struct {
	duration  *prometheus.HistogramVec
	queries   *prometheus.HistogramVec
	conflicts *prometheus.CounterVec
}

// tables bounds the statement label. Deriving a name from SQL text is only safe
// while the result set is closed, so an unrecognised target collapses to "other"
// rather than minting a series from whatever the statement happened to say.
var tables = map[string]string{
	"wallet": "wallet", "wager_transaction": "wager_transaction",
	"wallet_ledger_entry": "wallet_ledger_entry", "inbox": "inbox", "outbox": "outbox",
}

func (m *Metrics) SQLTracer() *SQLTracer {
	// Named per statement, because "wallet lock wait" and "wager lock wait" are
	// different questions: the first explains a slow BET, the second explains a
	// slow worker. One merged histogram answers neither.
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jungle_database_lock_query_seconds",
		Help:    "Duration of FOR UPDATE queries by statement, including lock wait and execution.",
		Buckets: financialBuckets,
	}, []string{"query"})
	// Every statement, not only the locking ones. Reconciliation sums a whole
	// ledger in NUMERIC and is plausibly the most expensive query here; until now
	// it was the one thing with no timing at all.
	q := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jungle_database_query_seconds",
		Help:    "Duration of every database statement, by bounded statement name.",
		Buckets: financialBuckets,
	}, []string{"statement"})
	m.Registry.MustRegister(h, q)
	return &SQLTracer{duration: h, queries: q, conflicts: m.Conflicts}
}

// statementName prefers the name sqlc writes into its generated constant and
// otherwise derives "verb:table" from the statement, which stays bounded because
// the table has to be one this schema owns.
func statementName(sql string) string {
	if named := queryName(sql); named != "adhoc" {
		return named
	}
	fields := strings.Fields(strings.ToLower(sql))
	// "wallet_ledger_entry(wallet_id," is one field, so the identifier has to be
	// cut at the first delimiter rather than merely trimmed at its ends.
	identifier := func(raw string) string {
		if cut := strings.IndexAny(raw, "(),;"); cut >= 0 {
			raw = raw[:cut]
		}
		return tables[raw]
	}
	// Transaction control is its own category, and commit is the interesting one:
	// the deferred integrity triggers run there, so its duration is the cost of
	// validating the write, not just the fsync.
	if len(fields) > 0 {
		switch fields[0] {
		case "begin", "commit", "rollback", "savepoint", "release", "prepare", "deallocate":
			return "tx:" + fields[0]
		}
	}
	verb, target := "other", ""
	for i, f := range fields {
		switch f {
		case "select", "insert", "delete", "with":
			if verb == "other" {
				verb = f
			}
		case "update":
			if verb == "other" {
				verb = f
			}
			if i+1 < len(fields) && target == "" {
				target = identifier(fields[i+1])
			}
		case "from", "into", "join":
			if i+1 < len(fields) && target == "" {
				target = identifier(fields[i+1])
			}
		}
	}
	if target == "" {
		return verb + ":other"
	}
	return verb + ":" + target
}

// queryName reads the statement name sqlc writes into the generated constant
// ("-- name: GetWalletForUpdate :one"). It is the cheap half of what otelpgx
// does, without taking on OpenTelemetry to get it. Hand-written statements carry
// no such comment and share one bounded fallback rather than inventing a label
// out of the SQL text, which would be unbounded.
func queryName(sql string) string {
	const marker = "-- name: "
	i := strings.Index(sql, marker)
	if i < 0 {
		return "adhoc"
	}
	rest := sql[i+len(marker):]
	if end := strings.IndexAny(rest, " \t\r\n"); end >= 0 {
		rest = rest[:end]
	}
	if rest == "" || len(rest) > 64 {
		return "adhoc"
	}
	return rest
}

type lockTiming struct {
	started   time.Time
	query     string
	statement string
	locking   bool
}

func (t *SQLTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, lockTimingKey{}, lockTiming{
		started:   time.Now(),
		query:     queryName(data.SQL),
		statement: statementName(data.SQL),
		locking:   strings.Contains(data.SQL, "FOR UPDATE"),
	})
}
func (t *SQLTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if timing, ok := ctx.Value(lockTimingKey{}).(lockTiming); ok {
		elapsed := time.Since(timing.started).Seconds()
		t.queries.WithLabelValues(timing.statement).Observe(elapsed)
		// The locking view is kept separate: it is the only one that includes
		// time spent queued behind another transaction rather than working.
		if timing.locking {
			t.duration.WithLabelValues(timing.query).Observe(elapsed)
		}
	}
	var failure *pgconn.PgError
	if errors.As(data.Err, &failure) && (failure.Code == "40001" || failure.Code == "40P01") {
		t.conflicts.WithLabelValues("postgres").Inc()
	}
}
