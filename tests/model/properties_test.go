//go:build integration

package model_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// assertIdentityAgreement is the idempotency property stated without reference
// to ordering: however many workers attempt one identity at once, they must all
// be told about the same operation.
//
// What "the same" means depends on the state, and the distinction is the
// contract rather than a tolerance. A terminal operation carries a frozen
// snapshot of the balance and version it produced, so a replay must return that
// snapshot for ever, even after later operations moved the wallet. A
// PENDING_REFERENCE operation has produced no result yet, so there is nothing to
// freeze and the engine answers with the live wallet instead: two replays a
// moment apart legitimately report different balances for the same pending
// operation. Requiring them to match would demand the engine invent a result it
// has not computed.
func assertIdentityAgreement(t *testing.T, seen map[string][]observation) {
	t.Helper()
	terminal := func(s string) bool { return s == "PROCESSED" || s == "REJECTED" || s == "FAILED" }
	disagreements := 0
	for id, answers := range seen {
		var first *observation
		for i := range answers {
			a := answers[i]
			if a.status == "" || a.code != "" {
				continue // refused before any durable outcome, nothing to compare
			}
			if first == nil {
				first = &answers[i]
				continue
			}
			// The identity must always name the same operation.
			if a.transactionID != first.transactionID {
				disagreements++
				if disagreements <= 5 {
					t.Errorf("identity %s named two operations: %s and %s", id, first.transactionID, a.transactionID)
				}
				continue
			}
			// PENDING_REFERENCE legitimately advances to a terminal state once a
			// worker resolves the reference; that is progress, not disagreement.
			if !terminal(a.status) || !terminal(first.status) {
				continue
			}
			if a.status != first.status || a.balance != first.balance ||
				a.version != first.version || a.failureCode != first.failureCode {
				disagreements++
				if disagreements <= 5 {
					t.Errorf("identity %s returned two different terminal results: %+v vs %+v", id, *first, a)
				}
			}
		}
	}
	if disagreements > 0 {
		t.Fatalf("%d identities disagreed about their own outcome", disagreements)
	}
}

// assertNoDuplicateEffect checks the property the client can verify from the
// outside: an identity is one operation, and one operation moves money at most
// once, no matter how many attempts raced for it.
func assertNoDuplicateEffect(t *testing.T, f fixture, seen map[string][]observation) {
	t.Helper()
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	var rows, duplicated, overEntered int64
	err := f.pool.QueryRow(context.Background(), `SELECT count(*), count(*) FILTER (WHERE hits > 1), count(*) FILTER (WHERE entries > 1)
	FROM (
	  SELECT t.external_transaction_id, count(DISTINCT t.id) AS hits, count(l.id) AS entries
	  FROM wager_transaction t
	  LEFT JOIN wallet_ledger_entry l ON l.transaction_id = t.id
	  WHERE t.provider_id = 'provider-a' AND t.external_transaction_id = ANY($1)
	  GROUP BY t.external_transaction_id
	) s`, ids).Scan(&rows, &duplicated, &overEntered)
	if err != nil {
		t.Fatal(err)
	}
	if duplicated != 0 {
		t.Fatalf("%d identities produced more than one transaction", duplicated)
	}
	if overEntered != 0 {
		t.Fatalf("%d identities produced more than one ledger entry", overEntered)
	}
	t.Logf("%d distinct identities persisted, none duplicated, none double-entered", rows)
}

// assertAuditorClean runs the same auditor the chaos suite uses as its verdict.
// The properties above are what a client can observe; this is what it cannot.
func assertAuditorClean(t *testing.T, f fixture, seed uint64) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	cmd := exec.Command("psql", os.Getenv("TEST_DATABASE_URL"), "-X", "-A", "-t", "-q",
		"-v", "ON_ERROR_STOP=1",
		"-v", "stale_event_age=2 minutes",
		"-v", "stale_reference_age=1 hour",
		"-F", "|", "-f", filepath.Join(root, "scripts/audit.sql"))
	cmd.Env = append(os.Environ(), "PGOPTIONS=-c search_path="+currentSchema(t, f))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("auditor did not run: %v\n%s", err, out)
	}
	var failures []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.HasPrefix(line, "FAIL|") {
			failures = append(failures, line)
		}
	}
	if len(failures) > 0 {
		if len(failures) > 20 {
			failures = failures[:20]
		}
		t.Fatalf("auditor found %d violation(s) after the generated run (replay with MODEL_SEED=%d):\n%s",
			len(failures), seed, strings.Join(failures, "\n"))
	}
	t.Log("auditor clean: every financial invariant held under the generated concurrency")
}

// currentSchema names the schema this run created. The auditor runs as a
// separate process, so it has to be pointed at it. The schema travels through
// PGOPTIONS rather than the URI: url.Values encodes a space as "+", libpq does
// not decode "+" back to a space, and the option arrives as a parameter literally
// called "+search_path".
func currentSchema(t *testing.T, f fixture) string {
	t.Helper()
	var schema string
	if err := f.pool.QueryRow(context.Background(), "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	return schema
}
