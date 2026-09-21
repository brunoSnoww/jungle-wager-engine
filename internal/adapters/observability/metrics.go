package observability

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"runtime"
	"strconv"
	"time"
)

// IDs, error messages, paths and financial amounts are never metric labels.
type Metrics struct {
	Registry                                     *prometheus.Registry
	Requests                                     *prometheus.CounterVec
	Latency                                      *prometheus.HistogramVec
	RouteDuration                                *prometheus.HistogramVec
	WagerDuration                                *prometheus.HistogramVec
	Results                                      *prometheus.CounterVec
	Duplicates, Retries, Conflicts               *prometheus.CounterVec
	OutboxPublished, Messages, ReferenceAttempts *prometheus.CounterVec
	Reconciliations                              *prometheus.CounterVec
	OutboxPending, OutboxAge, ReferencePending   prometheus.Gauge
	DLQDepth                                     *prometheus.GaugeVec
	ReconciliationMismatch                       prometheus.Counter
}

// financialBuckets span both regimes this engine actually has. An uncontended
// wager settles around 2ms, so the default Prometheus buckets -- whose lowest
// boundary is 5ms -- put the overwhelming majority of observations in a single
// bucket and make every percentile below it a straight-line guess. A wager
// queued behind others on the same wallet takes hundreds of milliseconds, so the
// range has to reach seconds as well. Measured, not copied: buckets borrowed
// from a slower service would be exactly as useless as the defaults.
var financialBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

func New() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(),
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jungle_http_requests_total", Help: "HTTP requests by bounded route and status."}, []string{"method", "route", "status"}),
		Latency:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "jungle_processing_seconds", Help: "Processing duration by entry point.", Buckets: financialBuckets}, []string{"transport"}),
		// The label set is declared, never inferred. kind and outcome are what the
		// latency question is actually asked about; provider_id would multiply the
		// series without answering "how long does a BET take".
		// The request counter already carried the route, so the count per route was
		// known and the duration was not: nobody could tell whether the ledger page
		// had become slow. status_class rather than status keeps a fast 404 from
		// being averaged into the latency of the work that actually runs.
		RouteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "jungle_http_request_seconds", Help: "HTTP request duration by bounded route.", Buckets: financialBuckets}, []string{"method", "route", "status_class"}),
		WagerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "jungle_wager_duration_seconds", Help: "Wager use case duration, excluding transport decoding and authorization.", Buckets: financialBuckets}, []string{"transport", "kind", "outcome"}),
		// failure_code is what turns one REJECTED bucket into an operational
		// story: a player short on funds is normal, a currency mismatch is a
		// broken integration, a missing reference is a queue ordering problem.
		// The code set is closed and small, so the cardinality is bounded.
		Results:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jungle_wager_results_total", Help: "Durable wager results by kind and failure code."}, []string{"transport", "kind", "status", "failure_code"}),
		Duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jungle_duplicates_total", Help: "Durable idempotent replays."}, []string{"transport"}),
		Retries:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jungle_retries_total", Help: "Retries by worker and reason."}, []string{"worker", "reason"}),
		// Backlog and age say how much is waiting; only a rate says whether it is
		// moving. Without this, "draining slowly" and "stopped draining" look the same.
		OutboxPublished:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jungle_outbox_published_total", Help: "Outbox publication attempts by event type and outcome."}, []string{"event_type", "outcome"}),
		Messages:               prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jungle_sqs_messages_total", Help: "Broker messages by queue and terminal outcome."}, []string{"queue", "outcome"}),
		ReferenceAttempts:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jungle_reference_attempts_total", Help: "Pending reference resolution attempts by outcome."}, []string{"outcome"}),
		Reconciliations:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jungle_reconciliations_total", Help: "Reconciliation checks by verdict."}, []string{"consistent"}),
		Conflicts:              prometheus.NewCounterVec(prometheus.CounterOpts{Name: "jungle_conflicts_total", Help: "Idempotency and concurrency conflicts."}, []string{"transport"}),
		OutboxPending:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "jungle_outbox_pending", Help: "Committed unpublished events."}),
		OutboxAge:              prometheus.NewGauge(prometheus.GaugeOpts{Name: "jungle_outbox_oldest_seconds", Help: "Age of oldest committed unpublished event."}),
		ReferencePending:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "jungle_references_pending", Help: "Durable reference waits."}),
		DLQDepth:               prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "jungle_dlq_messages", Help: "Approximate messages awaiting operator inspection, by dead letter queue."}, []string{"queue"}),
		ReconciliationMismatch: prometheus.NewCounter(prometheus.CounterOpts{Name: "jungle_reconciliation_mismatch_total", Help: "Read-only reconciliation mismatches."}),
	}
	m.Registry.MustRegister(m.Requests, m.Latency, m.RouteDuration, m.WagerDuration, m.Results, m.Duplicates, m.Retries, m.Conflicts, m.OutboxPublished, m.Messages, m.ReferenceAttempts, m.Reconciliations, m.OutboxPending, m.OutboxAge, m.ReferencePending, m.DLQDepth, m.ReconciliationMismatch, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// statusClass collapses the status to its family. Latency questions are asked
// per route, not per status code, and the full code would multiply every route's
// series by the number of codes it can return.
func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	default:
		return "2xx"
	}
}

// ObserveResult records a durable outcome. failure_code is empty for anything
// that was not rejected, which keeps the series count at one per real story.
func (m *Metrics) ObserveResult(transport, kind, status, failureCode string) {
	m.Results.WithLabelValues(transport, kind, status, failureCode).Inc()
}

// PoolCollector exposes connection pool saturation. This is the signal that was
// missing when the fleet exhausted PostgreSQL and every write became a 503: the
// backlog and latency metrics showed the symptom, nothing showed the cause.
func (m *Metrics) PoolCollector(stat func() *pgxpool.Stat) {
	gauge := func(name, help string, read func(*pgxpool.Stat) float64) {
		m.Registry.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{Name: name, Help: help},
			func() float64 { return read(stat()) }))
	}
	gauge("jungle_db_pool_connections", "Connections currently held by the pool.",
		func(s *pgxpool.Stat) float64 { return float64(s.TotalConns()) })
	gauge("jungle_db_pool_max_connections", "Ceiling this process will ever hold.",
		func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) })
	gauge("jungle_db_pool_acquired_connections", "Connections checked out right now.",
		func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) })
	// A caller that waited for a connection is the earliest warning that the
	// budget is wrong; it precedes the timeouts by a long way.
	gauge("jungle_db_pool_empty_acquires_total", "Acquires that had to wait for a free connection.",
		func(s *pgxpool.Stat) float64 { return float64(s.EmptyAcquireCount()) })
	gauge("jungle_db_pool_acquire_seconds_total", "Cumulative time spent waiting for a connection.",
		func(s *pgxpool.Stat) float64 { return s.AcquireDuration().Seconds() })
	gauge("jungle_db_pool_canceled_acquires_total", "Acquires abandoned because their context ended first.",
		func(s *pgxpool.Stat) float64 { return float64(s.CanceledAcquireCount()) })
}

// BuildInfo lets a regression be correlated with the deploy that introduced it.
func (m *Metrics) BuildInfo(version, instance string) {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "jungle_build_info", Help: "Always 1; the labels carry the build identity."}, []string{"version", "go_version", "instance"})
	m.Registry.MustRegister(g)
	g.WithLabelValues(version, runtime.Version(), instance).Set(1)
}

// ObserveWager records the financial path on its own, so a rejection that never
// touches the ledger is not averaged together with a debit that does, and a
// replay is not counted as the work it skipped.
func (m *Metrics) ObserveWager(transport, kind, outcome string, elapsed time.Duration) {
	m.WagerDuration.WithLabelValues(transport, kind, outcome).Observe(elapsed.Seconds())
}

func (m *Metrics) Observe(method, route string, status int, elapsed time.Duration) {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
	default:
		method = "OTHER"
	}
	m.Requests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.Latency.WithLabelValues("http").Observe(elapsed.Seconds())
	m.RouteDuration.WithLabelValues(method, route, statusClass(status)).Observe(elapsed.Seconds())
}
