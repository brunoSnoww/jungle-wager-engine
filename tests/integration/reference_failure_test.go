package integration_test

import (
	"context"
	"jungle/internal/application"
	"testing"
	"time"
)

func dueReference(t *testing.T, f fixture, id string) application.ReferenceClaim {
	t.Helper()
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, `UPDATE wager_transaction SET next_reference_attempt_at=clock_timestamp() WHERE id=$1 AND status='PENDING_REFERENCE'`, id); err != nil {
		t.Fatal(err)
	}
	claims, err := f.store.ClaimReferences(ctx, "failure-regression", 1, time.Minute)
	if err != nil || len(claims) != 1 || claims[0].TransactionID != id {
		t.Fatalf("claim: %+v %v", claims, err)
	}
	return claims[0]
}

func TestReferencePermanentFailureHasSnapshotAndNoFinancialEffects(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	refund := command(t, w, "refund-permanent", "REFUND", "10.00")
	refund.ReferenceExternalTransactionID = "bet-permanent"
	pending := process(t, f, refund)
	process(t, f, command(t, w, "bet-permanent", "BET", "10.00"))
	ctx := context.Background()
	// Deliberately damage only temporal metadata, simulating a persisted invariant
	// violation. The normal financial API never creates a future update time.
	if _, err := f.pool.Exec(ctx, `UPDATE wallet SET updated_at=clock_timestamp()+interval '1 hour' WHERE id=$1`, w.ID); err != nil {
		t.Fatal(err)
	}
	eventsBefore := count(t, f, `SELECT count(*) FROM outbox`)
	claim := dueReference(t, f, pending.TransactionID)
	first, err := f.store.RetryReference(ctx, claim, 2, time.Hour)
	if err != nil || first.Status != "PENDING_REFERENCE" {
		t.Fatalf("retry: %+v %v", first, err)
	}
	if count(t, f, `SELECT reference_attempts::bigint FROM wager_transaction WHERE id=$1`, pending.TransactionID) != 1 {
		t.Fatal("retry attempt was not durable")
	}
	if count(t, f, `SELECT count(*) FROM outbox`) != eventsBefore {
		t.Fatal("failed attempt emitted a financial event")
	}
	reconcile(t, f, w, "90.00")
	claim = dueReference(t, f, pending.TransactionID)
	failed, err := f.store.RetryReference(ctx, claim, 2, time.Hour)
	if err != nil || failed.Status != "FAILED" || failed.FailureCode != "PERMANENT_PROCESSING_FAILURE" || failed.Balance.String() != "90.00" || failed.WalletVersion != 2 {
		t.Fatalf("failed result: %+v %v", failed, err)
	}
	if count(t, f, `SELECT count(*) FROM wallet_ledger_entry WHERE transaction_id=$1`, pending.TransactionID) != 0 {
		t.Fatal("failed reference changed ledger")
	}
	if count(t, f, `SELECT count(*) FROM outbox WHERE event_type='WagerTransactionFailed' AND payload->'data'->>'transactionId'=$1`, pending.TransactionID) != 1 {
		t.Fatal("missing failure event")
	}
	if _, err = f.store.RetryReference(ctx, claim, 2, time.Hour); application.Code(err) != "CLAIM_LOST" {
		t.Fatalf("terminal reference was retried: %v", err)
	}
	if _, err = f.pool.Exec(ctx, `UPDATE wallet SET updated_at=clock_timestamp() WHERE id=$1`, w.ID); err != nil {
		t.Fatal(err)
	}
	process(t, f, command(t, w, "after-permanent", "WIN", "5.00"))
	replay := process(t, f, refund)
	if !replay.IdempotentReplay || replay.Status != "FAILED" || replay.Balance.String() != "90.00" || replay.WalletVersion != 2 {
		t.Fatalf("failed replay changed original result: %+v", replay)
	}
	reconcile(t, f, w, "95.00")
}

func TestReferenceDatabaseFailureRollsBackAndDoesNotBecomeFailed(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	refund := command(t, w, "refund-transient", "REFUND", "10.00")
	refund.ReferenceExternalTransactionID = "bet-transient"
	pending := process(t, f, refund)
	process(t, f, command(t, w, "bet-transient", "BET", "10.00"))
	ctx := context.Background()
	claim := dueReference(t, f, pending.TransactionID)
	if _, err := f.pool.Exec(ctx, `CREATE FUNCTION transient_test_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test database unavailable' USING ERRCODE='08006'; END $$; CREATE TRIGGER transient_test BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION transient_test_failure()`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.RetryReference(ctx, claim, 1, time.Hour); err == nil {
		t.Fatal("database failure must return retryable error")
	}
	if count(t, f, `SELECT reference_attempts::bigint FROM wager_transaction WHERE id=$1`, pending.TransactionID) != 0 {
		t.Fatal("database error committed an attempt")
	}
	if count(t, f, `SELECT count(*) FROM wager_transaction WHERE id=$1 AND status='PENDING_REFERENCE'`, pending.TransactionID) != 1 {
		t.Fatal("database error became terminal")
	}
	if count(t, f, `SELECT count(*) FROM wallet_ledger_entry WHERE transaction_id=$1`, pending.TransactionID) != 0 {
		t.Fatal("database error retained partial ledger")
	}
	reconcile(t, f, w, "90.00")
	if _, err := f.pool.Exec(ctx, `DROP TRIGGER transient_test ON outbox;DROP FUNCTION transient_test_failure()`); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.RetryReference(ctx, claim, 1, time.Hour)
	if err != nil || result.Status != "PROCESSED" {
		t.Fatalf("recovery: %+v %v", result, err)
	}
	reconcile(t, f, w, "100.00")
}
