//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// The ledger page orders on created_at, so whichever clock stamps it decides
// the order a provider sees. Stamping it in the application means three replicas
// stamp from three clocks, and two entries can then be ordered against the order
// the wallet lock actually granted.
//
// The assertion is inequality, not a sign or a window, because both of those
// answer to the skew of the moment rather than to the defect. This host was
// measured reading 16.4ms behind the server, and the work being timed takes
// single-digit milliseconds, so a sign test states the skew and a window test
// is wider than it. Either would pass or fail with whatever NTP did that
// morning, on an implementation that never changed.
//
// Inequality is skew-free. apply() takes one time.Now() and hands the identical
// value to the wager's completion and to the ledger insert, so the defect wrote
// the same instant into both columns. A stamp read from the server cannot
// reproduce a host reading except by landing on the same microsecond, which
// three entries make an accident worth ignoring.
func TestLedgerTimestampDoesNotComeFromThisProcess(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	for i := range 3 {
		process(t, f, command(t, w, fmt.Sprintf("clock-probe-%d", i), "BET", "1.00"))
	}

	rows, err := f.pool.Query(context.Background(),
		`SELECT x.external_transaction_id, e.created_at, x.completed_at
		   FROM wallet_ledger_entry e JOIN wager_transaction x ON x.id = e.transaction_id
		  WHERE e.wallet_id = $1 AND x.external_transaction_id LIKE 'clock-probe-%'`, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var external string
		var ledgerAt, completedAt time.Time
		if err := rows.Scan(&external, &ledgerAt, &completedAt); err != nil {
			t.Fatal(err)
		}
		seen++
		if ledgerAt.Equal(completedAt) {
			t.Fatalf("%s: ledger and wager completion both stamped %s, so the ledger carries this process's clock reading rather than the server's",
				external, ledgerAt.UTC())
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 3 {
		t.Fatalf("expected 3 probe entries, compared %d", seen)
	}
}

// The outbox column and the payload carry different timestamps on purpose. The
// payload says when the event happened; the consumer reads it and folds it into
// the inbox hash, so it must stay the domain value and stay stable across
// redeliveries. The column decides the order the publisher claims and sends in,
// and the broker groups FIFO by wallet, so an application clock there lets two
// replicas send one wallet's events in an order the database never committed
// them in.
//
// The payload value is this process's reading, which makes the same skew-free
// assertion available: a column stamped by the server cannot equal it.
func TestOutboxOrderingStampDoesNotComeFromThisProcess(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	process(t, f, command(t, w, "outbox-clock-probe", "BET", "1.00"))

	rows, err := f.pool.Query(context.Background(),
		`SELECT event_type, occurred_at, (payload->>'occurredAt')::timestamptz
		   FROM outbox WHERE aggregate_id = $1`, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var eventType string
		var column, inPayload time.Time
		if err := rows.Scan(&eventType, &column, &inPayload); err != nil {
			t.Fatal(err)
		}
		seen++
		if column.Equal(inPayload) {
			t.Fatalf("%s: the ordering column and the payload both read %s, so the publisher orders on this process's clock",
				eventType, column.UTC())
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen == 0 {
		t.Fatal("the wager wrote no outbox events, so nothing was compared")
	}
}
