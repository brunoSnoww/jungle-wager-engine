package integration_test

import (
	"context"
	"testing"
)

func TestLedgerInsertCannotAttachToCommittedLossOrRejection(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	loss := process(t, f, command(t, w, "loss-guard", "LOSS", "0.00"))
	rejected := process(t, f, command(t, w, "reject-guard", "BET", "200.00"))
	for _, id := range []string{loss.TransactionID, rejected.TransactionID} {
		_, err := f.pool.Exec(context.Background(), `INSERT INTO wallet_ledger_entry(wallet_id,transaction_id,direction,amount_minor,currency,balance_before_minor,balance_after_minor) VALUES($1,$2,'CREDIT',1,'BRL',0,1)`, w.ID, id)
		if err == nil {
			t.Fatalf("committed nonfinancial wager accepted a ledger entry: %s", id)
		}
		if count(t, f, `SELECT count(*) FROM wallet_ledger_entry WHERE transaction_id=$1`, id) != 0 {
			t.Fatal("invalid ledger persisted")
		}
	}
	reconcile(t, f, w, "100.00")
}

func TestLedgerDirectionMustMatchWagerKind(t *testing.T) {
	f := database(t)
	w := wallet(t, f, "100.00")
	ctx := context.Background()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var id string
	// This intentionally satisfies amount, equation, FK, and result checks. Only
	// the direction-to-kind guard distinguishes this credit from a valid BET.
	err = tx.QueryRow(ctx, `INSERT INTO wager_transaction(origin,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount_minor,currency,status,result_balance_minor,result_currency,result_wallet_version,correlation_id,completed_at)
 VALUES('EXTERNAL','provider-a','wrong-direction','wrong-direction',decode(repeat('a',64),'hex'),$1,$2,'round-1','game-1','BET',1000,'BRL','PROCESSED',11000,'BRL',2,'direction-test',now()) RETURNING id::text`, w.ID, w.PlayerID).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO wallet_ledger_entry(wallet_id,transaction_id,direction,amount_minor,currency,balance_before_minor,balance_after_minor) VALUES($1,$2,'CREDIT',1000,'BRL',10000,11000)`, w.ID, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err == nil {
		t.Fatal("BET accepted a CREDIT ledger")
	}
	reconcile(t, f, w, "100.00")
}
