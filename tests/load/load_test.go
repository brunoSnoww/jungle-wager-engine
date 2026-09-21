//go:build load

package load

import (
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// report is the per-status breakdown the default success rate cannot express:
// a 422 INSUFFICIENT_FUNDS is the engine working, not a failure.
type report struct {
	codes      map[uint16]int64
	errors     map[string]int64
	requests   int64
	conflicts  float64
	duplicates float64
	metrics    vegeta.Metrics
}

func (r *report) log(t *testing.T, name string, offered int) {
	t.Helper()
	r.metrics.Close()
	statuses := make([]int, 0, len(r.codes))
	for code := range r.codes {
		statuses = append(statuses, int(code))
	}
	sort.Ints(statuses)
	breakdown := ""
	for _, code := range statuses {
		breakdown += fmt.Sprintf(" %d=%d", code, r.codes[uint16(code)])
	}
	t.Logf("[%s] offered=%d/s achieved=%.1f/s requests=%d duration=%s",
		name, offered, r.metrics.Rate, r.requests, r.metrics.Duration.Round(time.Millisecond))
	t.Logf("[%s] latency p50=%s p95=%s p99=%s max=%s",
		name, r.metrics.Latencies.P50.Round(time.Millisecond), r.metrics.Latencies.P95.Round(time.Millisecond),
		r.metrics.Latencies.P99.Round(time.Millisecond), r.metrics.Latencies.Max.Round(time.Millisecond))
	t.Logf("[%s] status:%s", name, breakdown)
	// Always reported, including at zero: a rate achieved by losing races is not
	// the same result as the same rate achieved without them, and a report that
	// omits the field when it is clean cannot be distinguished from one that
	// never measured it.
	t.Logf("[%s] concurrency: conflicts=%.0f idempotent_replays=%.0f", name, r.conflicts, r.duplicates)
	for message, count := range r.errors {
		t.Logf("[%s] transport error %q x%d", name, message, count)
	}
}

// assertDelivered bounds requests that never got a response. Without it a run
// that lost most of its traffic in transit still prints "invariant held" over a
// measurement of almost nothing.
func (r *report) assertDelivered(t *testing.T, name string) {
	t.Helper()
	var lost int64
	for _, n := range r.errors {
		lost += n
	}
	if r.requests == 0 || float64(lost)/float64(r.requests) > 0.01 {
		t.Fatalf("[%s] %d of %d requests never received a response: the run measured too little to trust", name, lost, r.requests)
	}
}

// attack runs one open-model constant-rate burst. Open model matters: a closed
// generator waits for each response and silently throttles itself, hiding the
// queue growth that saturation actually looks like.
func (l *lab) attack(name string, targeter vegeta.Targeter, rate int, duration time.Duration) *report {
	l.t.Helper()
	attacker := vegeta.NewAttacker(
		vegeta.Timeout(30*time.Second),
		vegeta.KeepAlive(true),
		vegeta.Workers(uint64(rate)),
		vegeta.MaxWorkers(uint64(rate)*4),
		vegeta.Connections(4096),
	)
	defer attacker.Stop()
	r := &report{codes: map[uint16]int64{}, errors: map[string]int64{}}
	conflictsBefore := l.counter("jungle_conflicts_total")
	duplicatesBefore := l.counter("jungle_duplicates_total")
	pacer := vegeta.ConstantPacer{Freq: rate, Per: time.Second}
	for res := range attacker.Attack(targeter, pacer, duration, name) {
		r.requests++
		r.codes[res.Code]++
		// Vegeta reports every non-2xx as an error. Here a 422 is the engine
		// refusing a bet on purpose, so only a missing response is an error.
		if res.Code == 0 && res.Error != "" {
			r.errors[res.Error]++
		}
		r.metrics.Add(res)
	}
	r.conflicts = l.counter("jungle_conflicts_total") - conflictsBefore
	r.duplicates = l.counter("jungle_duplicates_total") - duplicatesBefore
	return r
}

// external builds the run-scoped identity shared by the body and the header,
// so every request is a genuinely new operation rather than a replay.
func (l *lab) external(seq uint64) string { return fmt.Sprintf("load-%s-%d", l.run, seq) }

func (l *lab) bet(w wallet, seq uint64, amount string) []byte {
	return []byte(fmt.Sprintf(
		`{"providerId":"provider-a","externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"load","gameId":"load","kind":"BET","money":{"amount":%q,"currency":"BRL"}}`,
		l.external(seq), w.PlayerID, w.ID, amount))
}

// TestDrainOneWalletUnderLoad is the headline: a wallet funded for exactly K
// bets, hammered with far more than K concurrent bets across three replicas.
// Correctness is read from PostgreSQL, not from the responses, so a dropped
// response cannot disguise a double debit.
func TestDrainOneWalletUnderLoad(t *testing.T) {
	l := newLab(t)
	offered := l.settings.rate
	attempts := offered * int(l.settings.duration/time.Second)
	if attempts < 40 {
		t.Fatalf("raise LOAD_RATE or LOAD_DURATION: %d attempts cannot demonstrate a drain", attempts)
	}
	// Fund for a quarter of the offered attempts: the rest must be rejected.
	// Both strings derive from unitCents so the two cannot drift apart.
	const unitCents = 10
	affordable := attempts / 4
	unit := cents(unitCents)
	w := l.openWallet(cents(affordable * unitCents))
	t.Logf("wallet %s funded for exactly %d bets of %s", w.ID, affordable, unit)

	var seq atomic.Uint64
	targeter := func(tgt *vegeta.Target) error {
		n := seq.Add(1)
		tgt.Method = "POST"
		tgt.URL = l.target("/wagering/transactions")
		tgt.Body = l.bet(w, n, unit)
		tgt.Header = map[string][]string{
			"Authorization":   {l.provider.bearer()},
			"Content-Type":    {"application/json"},
			"Idempotency-Key": {"provider-a:" + l.external(n)},
		}
		return nil
	}
	r := l.attack("drain", targeter, offered, l.settings.duration)
	r.log(t, "drain", offered)
	l.assertTokensHeld()
	r.assertDelivered(t, "drain")

	// Saturation is the point of this profile, so distinguish "the engine refused
	// correctly" from "the generator outran the engine". Only the first is a
	// verdict about the wallet; the second is a load-shaping problem and must not
	// masquerade as a double-debit.
	if reached := r.codes[201] + r.codes[422]; reached <= int64(affordable) {
		t.Fatalf("only %d requests reached the engine for a wallet funded for %d: the generator outran the service, raise LOAD_DURATION or lower LOAD_RATE", reached, affordable)
	}
	// Vegeta returns when its workers drain, but a request whose response the
	// client gave up on can still be committing, so converge before asserting.
	processed := l.awaitScalar(int64(affordable), 30*time.Second,
		`SELECT count(*) FROM wager_transaction WHERE wallet_id=$1 AND status='PROCESSED' AND kind='BET'`, w.ID)
	debits := l.scalar(`SELECT count(*) FROM wallet_ledger_entry WHERE wallet_id=$1 AND direction='DEBIT'`, w.ID)
	if processed != int64(affordable) {
		t.Fatalf("funded for %d bets but %d were processed", affordable, processed)
	}
	if debits != int64(affordable) {
		t.Fatalf("processed=%d but ledger holds %d debits", processed, debits)
	}
	rec := l.reconcile(w.ID)
	if rec.StoredBalance.Amount != "0.00" {
		t.Fatalf("drained wallet should hold 0.00, holds %s", rec.StoredBalance.Amount)
	}
	l.assertConsistent(w)
	l.awaitOutboxDrained(2 * time.Minute)
	t.Logf("[drain] invariant held: %d processed, %d debits, balance 0.00, reconciliation exact", processed, debits)
}

// TestFanoutThroughput measures the honest ceiling. Distinct wallets do not
// contend on the same row, so this is the number worth quoting; the drain
// profile above is the contention case, not the throughput case.
func TestFanoutThroughput(t *testing.T) {
	l := newLab(t)
	wallets := make([]wallet, 0, l.settings.wallets)
	for range l.settings.wallets {
		// Funded far beyond the run so throughput is not truncated by rejections.
		wallets = append(wallets, l.openWallet("1000000.00"))
	}
	t.Logf("opened %d independent wallets", len(wallets))

	var seq atomic.Uint64
	targeter := func(tgt *vegeta.Target) error {
		n := seq.Add(1)
		w := wallets[int(n)%len(wallets)]
		tgt.Method = "POST"
		tgt.URL = l.target("/wagering/transactions")
		tgt.Body = l.bet(w, n, "1.00")
		tgt.Header = map[string][]string{
			"Authorization":   {l.provider.bearer()},
			"Content-Type":    {"application/json"},
			"Idempotency-Key": {"provider-a:" + l.external(n)},
		}
		return nil
	}
	r := l.attack("fanout", targeter, l.settings.rate, l.settings.duration)
	r.log(t, "fanout", l.settings.rate)
	l.assertTokensHeld()
	r.assertDelivered(t, "fanout")

	if created := r.codes[201]; created == 0 {
		t.Fatal("no wager was processed; the run measured nothing")
	}
	for code, count := range r.codes {
		if code >= 500 {
			t.Errorf("%d responses with status %d: the service failed rather than rejected", count, code)
		}
	}
	l.assertConsistent(wallets...)
	l.awaitOutboxDrained(3 * time.Minute)
	t.Logf("[fanout] invariant held across %d wallets: every reconciliation exact", len(wallets))
}
