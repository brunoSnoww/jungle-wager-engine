package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"jungle/internal/application"
)

// The ledger cursor is a read contract with three separate promises: it pages,
// it orders (created_at, id) DESC, and it stays stable while the ledger grows.
// Each is asserted on its own, because a test that only counts distinct ids
// passes against an implementation that does no paging at all.
func TestLedgerCursorPagesInOrderAndStaysStable(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "1000.00")
	const bets, limit = 9, 2

	want := map[string]bool{}
	for id := range ledgerIDs(t, f, w.ID) { // the OPENING credit
		want[id] = true
	}
	for i := range bets {
		process(t, f, command(t, w, fmt.Sprintf("bet-%d", i), "BET", "1.00"))
	}
	for id := range ledgerIDs(t, f, w.ID) {
		want[id] = true
	}
	lateArrival := ""

	seen := map[string]int{}
	var walk []application.LedgerEntry
	cursor, pages := "", 0
	for {
		p, err := f.store.Ledger(context.Background(), w.ID, cursor, limit)
		if err != nil {
			t.Fatal(err)
		}
		// Without this the whole walk can be one page and every other assertion
		// still passes, which is exactly how a no-op implementation slips through.
		if len(p.Entries) > limit {
			t.Fatalf("page returned %d entries against limit %d", len(p.Entries), limit)
		}
		for _, e := range p.Entries {
			seen[e.ID]++
			walk = append(walk, e)
		}
		pages++
		if pages == 1 {
			// A later entry sorts ahead of the cursor in a DESC walk. The cursor
			// has to be blind to it: it may appear, but it may not displace or
			// duplicate anything that was already there.
			r := process(t, f, command(t, w, "late-arrival", "BET", "1.00"))
			lateArrival = ledgerIDFor(t, f, r.TransactionID)
		}
		if p.NextCursor == "" {
			break
		}
		if p.NextCursor == cursor {
			t.Fatal("cursor did not advance")
		}
		cursor = p.NextCursor
		if pages > 20 {
			t.Fatal("cursor never terminated")
		}
	}

	if pages < (bets+1)/limit {
		t.Fatalf("walked %d pages for %d entries at limit %d: pagination did not happen", pages, bets+1, limit)
	}
	// Exact set, not a floor: a floor lets one skipped original hide behind the
	// late arrival, because both leave the total unchanged.
	for id := range want {
		if seen[id] != 1 {
			t.Errorf("entry %s appeared %d times, want exactly 1", id, seen[id])
		}
	}
	for id := range seen {
		if !want[id] && id != lateArrival {
			t.Errorf("entry %s appeared but was never expected", id)
		}
	}
	// Half the contract is the ordering, and a map cannot see it.
	for i := 1; i < len(walk); i++ {
		prev, cur := walk[i-1], walk[i]
		if cur.CreatedAt.After(prev.CreatedAt) || (cur.CreatedAt.Equal(prev.CreatedAt) && cur.ID > prev.ID) {
			t.Fatalf("walk is not (created_at, id) DESC at position %d: %s then %s", i, prev.ID, cur.ID)
		}
	}
}

// created_at defaults to now(), which is transaction start time, so entries
// written in one transaction share it exactly. That tie is the only input the
// id tiebreak in the row comparator exists for, and a walk of one-wager-per-
// transaction never produces it.
func TestLedgerCursorSurvivesIdenticalTimestamps(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "1000.00")
	tied := insertTiedLedgerEntries(t, f, w, 5)

	seen := map[string]int{}
	cursor := ""
	for {
		p, err := f.store.Ledger(context.Background(), w.ID, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range p.Entries {
			seen[e.ID]++
		}
		if p.NextCursor == "" || p.NextCursor == cursor {
			break
		}
		cursor = p.NextCursor
	}
	for _, id := range tied {
		if seen[id] != 1 {
			t.Errorf("tied entry %s appeared %d times, want exactly 1: the id tiebreak is not holding", id, seen[id])
		}
	}
}

func TestLedgerCursorRejectsTamperingAndForeignWallets(t *testing.T) {
	f := database(t)
	mine := wallet(t, f, "100.00")
	theirs := wallet(t, f, "100.00")
	process(t, f, command(t, mine, "mine-1", "BET", "1.00"))
	process(t, f, command(t, theirs, "theirs-1", "BET", "1.00"))

	first, err := f.store.Ledger(context.Background(), mine.ID, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.NextCursor == "" {
		t.Fatal("expected a continuation cursor")
	}
	// Positive control: without it, "reject every cursor" would pass the table.
	if _, err := f.store.Ledger(context.Background(), mine.ID, first.NextCursor, 2); err != nil {
		t.Fatalf("a cursor from this wallet was rejected: %v", err)
	}

	for name, c := range map[string]struct{ wallet, cursor string }{
		"not base64":    {mine.ID, "!!!not-base64!!!"},
		"empty payload": {mine.ID, "e30"},
		"plain text":    {mine.ID, "aGVsbG8"},
		// Valid base64 alphabet, so only the length guard can reject it.
		"oversized":      {mine.ID, strings.Repeat("A", 600)},
		"another wallet": {theirs.ID, first.NextCursor},
	} {
		_, err := f.store.Ledger(context.Background(), c.wallet, c.cursor, 2)
		var e *application.Error
		if !errors.As(err, &e) || e.Code != "INVALID_CURSOR" {
			t.Errorf("%s: got %v, want INVALID_CURSOR", name, err)
		}
	}
}

func ledgerIDs(t *testing.T, f fixture, walletID string) map[string]bool {
	t.Helper()
	rows, err := f.pool.Query(context.Background(), `SELECT id::text FROM wallet_ledger_entry WHERE wallet_id=$1`, walletID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids[id] = true
	}
	return ids
}

func ledgerIDFor(t *testing.T, f fixture, transactionID string) string {
	t.Helper()
	var id string
	if err := f.pool.QueryRow(context.Background(), `SELECT id::text FROM wallet_ledger_entry WHERE transaction_id=$1`, transactionID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// insertTiedLedgerEntries writes n wagers and their entries in a single
// transaction so every created_at is identical. The store cannot produce this
// shape -- it commits one wager per transaction -- but a batched writer would,
// and the row comparator has to cope either way.
func insertTiedLedgerEntries(t *testing.T, f fixture, w application.WalletView, n int) []string {
	t.Helper()
	ctx := context.Background()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	ids := make([]string, 0, n)
	balance := int64(100000)
	for i := range n {
		var wagerID string
		external := fmt.Sprintf("tied-%d", i)
		if err := tx.QueryRow(ctx, `INSERT INTO wager_transaction(origin,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount_minor,currency,status,result_balance_minor,result_currency,result_wallet_version,correlation_id,completed_at)
 VALUES('EXTERNAL','provider-a',$3,$3,decode(repeat('b',64),'hex'),$1,$2,'round','game','BET',100,'BRL','PROCESSED',$4,'BRL',$5,'tied',now()) RETURNING id::text`,
			w.ID, w.PlayerID, external, balance-100, int64(i+2)).Scan(&wagerID); err != nil {
			t.Fatal(err)
		}
		var entryID string
		if err := tx.QueryRow(ctx, `INSERT INTO wallet_ledger_entry(wallet_id,transaction_id,direction,amount_minor,currency,balance_before_minor,balance_after_minor)
 VALUES($1,$2,'DEBIT',100,'BRL',$3,$4) RETURNING id::text`, w.ID, wagerID, balance, balance-100).Scan(&entryID); err != nil {
			t.Fatal(err)
		}
		balance -= 100
		ids = append(ids, entryID)
	}
	if _, err := tx.Exec(ctx, `UPDATE wallet SET balance_minor=$2, version=$3 WHERE id=$1`, w.ID, balance, int64(n+1)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return ids
}
