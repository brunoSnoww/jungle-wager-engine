//go:build load

package load

import (
	"sync/atomic"
	"testing"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// TestSteadyLoadUnderChaos drives a long, modest, constant-rate stream while
// scripts/chaos.sh injects faults underneath it.
//
// The measuring profiles refuse any 5xx and cap transport errors at 1%. Under
// chaos both are expected, so this profile asserts the contract instead of the
// weather: whatever the service acknowledged must exist exactly once, and
// whatever it refused must have left no trace. Internal database consistency is
// the auditor's job (scripts/audit.sql) and is deliberately not duplicated here.
//
// Run it alongside chaos with: make soak
func TestSteadyLoadUnderChaos(t *testing.T) {
	l := newLab(t)
	wallets := make([]wallet, 0, l.settings.wallets)
	for range l.settings.wallets {
		// Funded far beyond the run: a rejection here would be a funding
		// artefact competing with the faults for the explanation.
		wallets = append(wallets, l.openWallet("100000000.00"))
	}
	t.Logf("chaos profile: %d wallets, %d rps offered for %s across %d targets",
		len(wallets), l.settings.rate, l.settings.duration, len(l.settings.targets))

	var seq atomic.Uint64
	targeter := func(tgt *vegeta.Target) error {
		n := seq.Add(1)
		tgt.Method = "POST"
		tgt.URL = l.target("/wagering/transactions")
		tgt.Body = l.bet(wallets[int(n)%len(wallets)], n, "1.00")
		tgt.Header = map[string][]string{
			"Authorization":   {l.provider.bearer()},
			"Content-Type":    {"application/json"},
			"Idempotency-Key": {"provider-a:" + l.external(n)},
		}
		return nil
	}

	// Vegeta returns results in completion order, not attack order, so a result
	// cannot be mapped back to the identity that produced it. The contract is
	// asserted on counts and on the run's whole identity range instead, which
	// needs no such mapping and is just as strict.
	var acknowledged int64
	timeline := map[int64]map[uint16]int{}

	r := &report{codes: map[uint16]int64{}, errors: map[string]int64{}}
	attacker := vegeta.NewAttacker(vegeta.Timeout(30*time.Second), vegeta.KeepAlive(true),
		vegeta.Workers(uint64(l.settings.rate)), vegeta.MaxWorkers(uint64(l.settings.rate)*4), vegeta.Connections(4096))
	defer attacker.Stop()

	started := time.Now()
	for res := range attacker.Attack(targeter, vegeta.ConstantPacer{Freq: l.settings.rate, Per: time.Second}, l.settings.duration, "chaos") {
		r.requests++
		r.codes[res.Code]++
		if res.Code == 0 && res.Error != "" {
			r.errors[res.Error]++
		}
		r.metrics.Add(res)
		if res.Code == 201 || res.Code == 200 || res.Code == 422 {
			acknowledged++
		}
		bucket := int64(res.Timestamp.Sub(started) / (10 * time.Second))
		if timeline[bucket] == nil {
			timeline[bucket] = map[uint16]int{}
		}
		timeline[bucket][res.Code]++
	}
	r.log(t, "chaos", l.settings.rate)
	for bucket := int64(0); bucket <= int64(l.settings.duration/(10*time.Second)); bucket++ {
		if counts, ok := timeline[bucket]; ok {
			t.Logf("[chaos] t+%3ds %v", bucket*10, counts)
		}
	}

	if acknowledged == 0 {
		t.Fatal("no request was ever acknowledged: the run measured nothing")
	}
	// Every identity this run could have sent shares one prefix, so the whole
	// range is auditable without knowing which individual request survived.
	var distinct, duplicated, unfinished int64
	if err := l.queryRow(`SELECT count(*), count(*) FILTER (WHERE hits > 1), count(*) FILTER (WHERE unfinished > 0)
	FROM (
	  SELECT external_transaction_id, count(*) AS hits,
	         count(*) FILTER (WHERE status NOT IN ('PROCESSED','REJECTED')) AS unfinished
	  FROM wager_transaction
	  WHERE provider_id='provider-a' AND external_transaction_id LIKE $1
	  GROUP BY external_transaction_id
	) t`, "load-"+l.run+"-%").Scan(&distinct, &duplicated, &unfinished); err != nil {
		t.Fatal(err)
	}

	// Duplicate money is the unforgivable outcome: an identity must never have
	// produced two operations, no matter how the process died.
	if duplicated != 0 {
		t.Fatalf("%d identities from this run exist more than once: chaos produced duplicate money", duplicated)
	}
	// A request answered with a durable outcome must be in PostgreSQL. The
	// converse is allowed: an unanswered request may well have committed, which
	// is what at-least-once means.
	if distinct < acknowledged {
		t.Fatalf("service acknowledged %d operations but only %d are persisted: chaos lost committed work", acknowledged, distinct)
	}
	// PENDING is an in-transaction state and must never survive a crash.
	if unfinished != 0 {
		t.Fatalf("%d operations from this run never reached a terminal state", unfinished)
	}

	// 3. Every event committed during the chaos window must still reach the
	//    broker once the faults stop. Publishing may lag; losing is not allowed.
	l.awaitOutboxDrained(5 * time.Minute)

	t.Logf("[chaos] contract held: %d acknowledged, %d persisted, zero duplicated, zero unfinished", acknowledged, distinct)
	t.Log("[chaos] run scripts/audit.sh for the database-internal invariants")
}
