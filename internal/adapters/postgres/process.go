package postgres

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"jungle/internal/application"
	"jungle/internal/domain"
)

func (s *Store) ProcessWager(ctx context.Context, c application.Command, inbox *application.InboxMessage) (application.Result, error) {
	c, err := application.NormalizeCommand(c)
	if err != nil {
		return application.Result{}, err
	}
	hash, err := application.PayloadHash(c)
	if err != nil {
		return application.Result{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.Result{}, dbError(err)
	}
	defer tx.Rollback(ctx)
	if inbox != nil {
		existing, e := reserveInbox(ctx, tx, *inbox)
		if e != nil {
			return application.Result{}, e
		}
		if existing != "" {
			d, e := scanWager(tx.QueryRow(ctx, "SELECT "+wagerColumns+" FROM wager_transaction t WHERE t.id=$1", existing))
			if e != nil {
				return application.Result{}, dbError(e)
			}
			// Even durable inbox replay remains scoped to the authenticated provider.
			if d.ProviderID != c.ProviderID {
				return application.Result{}, &application.Error{Code: "INBOX_MESSAGE_REUSED"}
			}
			w, e := scanWallet(tx.QueryRow(ctx, "SELECT "+walletColumns+" FROM wallet WHERE id=$1", d.WalletID.String()))
			if e != nil {
				return application.Result{}, dbError(e)
			}
			if e = tx.Commit(ctx); e != nil {
				return application.Result{}, dbError(e)
			}
			return resultOf(d, w, true), nil
		}
	}
	if d, found, e := findIdentity(ctx, tx, c, hash); e != nil {
		return application.Result{}, e
	} else if found {
		return commitReplay(ctx, tx, c, inbox, d)
	}
	w, err := lockWallet(ctx, tx, c.WalletID)
	if err != nil {
		return application.Result{}, dbError(err)
	}
	if w.PlayerID.String() != c.PlayerID {
		return application.Result{}, &application.Error{Code: "PLAYER_MISMATCH"}
	}
	id, err := allocateID(ctx, tx)
	if err != nil {
		return application.Result{}, dbError(err)
	}
	pid, _ := domain.ParseID(c.PlayerID)
	now := time.Now().UTC()
	wg, err := domain.NewWager(domain.WagerData{ID: domain.WagerTransactionID(id), WalletID: w.ID, PlayerID: domain.PlayerID(pid), ProviderID: c.ProviderID, ExternalTransactionID: c.ExternalTransactionID, IdempotencyKey: c.IdempotencyKey, PayloadHash: hash, RoundID: c.RoundID, GameID: c.GameID, Kind: domain.Kind(c.Kind), Money: c.Money, ReferenceExternalTransactionID: c.ReferenceExternalTransactionID, Status: domain.StatusPending, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		return application.Result{}, err
	}
	var inserted string
	err = tx.QueryRow(ctx, `INSERT INTO wager_transaction(id,origin,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount_minor,currency,reference_external_transaction_id,status,correlation_id,causation_id,created_at,updated_at) VALUES($1,'EXTERNAL',$2,$3,$4,decode($5,'hex'),$6,$7,$8,$9,$10,$11,$12,$13,'PENDING',$14,$15,$16,$16) ON CONFLICT DO NOTHING RETURNING id::text`, id.String(), c.ProviderID, c.ExternalTransactionID, c.IdempotencyKey, hash, c.WalletID, c.PlayerID, c.RoundID, c.GameID, c.Kind, c.Money.Minor(), c.Money.Currency(), optional(c.ReferenceExternalTransactionID), c.CorrelationID, optional(c.CausationID), now).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		// This second command receives a fresh READ COMMITTED snapshot after waiting
		// for the winner. Never continue a transaction aborted by unique violation.
		d, found, e := findIdentity(ctx, tx, c, hash)
		if e != nil {
			return application.Result{}, e
		}
		if !found {
			return application.Result{}, &application.Error{Code: "UNAVAILABLE"}
		}
		return commitReplay(ctx, tx, c, inbox, d)
	}
	if err != nil {
		return application.Result{}, dbError(err)
	}
	result, err := apply(ctx, tx, w, wg, c.CorrelationID, c.CausationID, false)
	if err != nil {
		return application.Result{}, err
	}
	if err = completeInbox(ctx, tx, inbox, result.TransactionID); err != nil {
		return application.Result{}, dbError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return application.Result{}, dbError(err)
	}
	return result, nil
}

// findIdentity resolves both business identities in a single statement, so both
// are read from one snapshot. Two sequential SELECTs under READ COMMITTED can
// straddle a competitor's commit: the idempotency lookup misses, the external
// lookup then finds that very same row, and a legitimate replay is reported as
// an external transaction conflict. The key match is preferred when both hit.
func findIdentity(ctx context.Context, tx pgx.Tx, c application.Command, hash string) (domain.WagerData, bool, error) {
	// UNION ALL rather than OR: each arm is pinned to its own unique index, so a
	// generic plan cannot degrade the hot path into scanning a provider's whole
	// history. Still one statement, which is what removes the race.
	d, err := scanWager(tx.QueryRow(ctx, "SELECT "+wagerColumns+` FROM (
 SELECT 1 AS preference, w.* FROM wager_transaction w WHERE w.provider_id=$1 AND w.idempotency_key=$2
 UNION ALL
 SELECT 2, w.* FROM wager_transaction w WHERE w.provider_id=$1 AND w.external_transaction_id=$3
 ) t ORDER BY t.preference LIMIT 1`, c.ProviderID, c.IdempotencyKey, c.ExternalTransactionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return d, false, nil
	}
	if err != nil {
		return d, false, dbError(err)
	}
	if d.IdempotencyKey != c.IdempotencyKey {
		return d, false, &application.Error{Code: "EXTERNAL_TRANSACTION_ID_REUSED"}
	}
	if d.PayloadHash != hash {
		return d, false, &application.Error{Code: "IDEMPOTENCY_KEY_REUSED"}
	}
	return d, true, nil
}

func commitReplay(ctx context.Context, tx pgx.Tx, c application.Command, inbox *application.InboxMessage, d domain.WagerData) (application.Result, error) {
	w, err := scanWallet(tx.QueryRow(ctx, "SELECT "+walletColumns+" FROM wallet WHERE id=$1", d.WalletID.String()))
	if err != nil {
		return application.Result{}, dbError(err)
	}
	if err = completeInbox(ctx, tx, inbox, d.ID.String()); err != nil {
		return application.Result{}, dbError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return application.Result{}, dbError(err)
	}
	return resultOf(d, w, true), nil
}

func reserveInbox(ctx context.Context, tx pgx.Tx, m application.InboxMessage) (string, error) {
	hash, err := hex.DecodeString(m.PayloadHash)
	if err != nil || len(hash) != 32 || m.ConsumerName == "" || m.MessageID == "" {
		return "", &application.Error{Code: "INVALID_INPUT"}
	}
	var id string
	err = tx.QueryRow(ctx, `INSERT INTO inbox(consumer_name,message_id,payload_hash) VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING message_id`, m.ConsumerName, m.MessageID, hash).Scan(&id)
	if err == nil {
		return "", nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", dbError(err)
	}
	var stored string
	var completed *time.Time
	var wager *string
	err = tx.QueryRow(ctx, `SELECT encode(payload_hash,'hex'),wager_transaction_id::text,completed_at FROM inbox WHERE consumer_name=$1 AND message_id=$2`, m.ConsumerName, m.MessageID).Scan(&stored, &wager, &completed)
	if err != nil {
		return "", dbError(err)
	}
	if stored != m.PayloadHash {
		return "", &application.Error{Code: "INBOX_MESSAGE_REUSED"}
	}
	if wager == nil || completed == nil {
		return "", &application.Error{Code: "UNAVAILABLE"}
	}
	return *wager, nil
}
func completeInbox(ctx context.Context, tx pgx.Tx, m *application.InboxMessage, transactionID string) error {
	if m == nil {
		return nil
	}
	_, err := tx.Exec(ctx, `UPDATE inbox SET wager_transaction_id=$3,completed_at=clock_timestamp() WHERE consumer_name=$1 AND message_id=$2 AND completed_at IS NULL`, m.ConsumerName, m.MessageID, transactionID)
	return err
}

func apply(ctx context.Context, tx pgx.Tx, w domain.WalletData, wg domain.WagerTransaction, correlation, causation string, expired bool) (application.Result, error) {
	d := wg.Snapshot()
	wasPendingReference := d.Status == domain.StatusPendingReference
	var reference *domain.WagerData
	reversed := false
	if d.ReferenceExternalTransactionID != "" {
		ref, err := scanWager(tx.QueryRow(ctx, "SELECT "+wagerColumns+" FROM wager_transaction t WHERE provider_id=$1 AND external_transaction_id=$2", d.ProviderID, d.ReferenceExternalTransactionID))
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return application.Result{}, dbError(err)
		}
		if err == nil {
			// Same-wallet writers already serialize on our wallet lock. Never lock a
			// foreign wallet/reference: domain will reject identity mismatch.
			if ref.WalletID == w.ID {
				ref, err = scanWager(tx.QueryRow(ctx, "SELECT "+wagerColumns+" FROM wager_transaction t WHERE id=$1 FOR UPDATE", ref.ID.String()))
				if err != nil {
					return application.Result{}, dbError(err)
				}
			}
			reference = &ref
			if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM wager_transaction WHERE reference_transaction_id=$1 AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED')`, ref.ID.String()).Scan(&reversed); err != nil {
				return application.Result{}, dbError(err)
			}
		}
	}
	decision, err := domain.Evaluate(w, d, reference, reversed)
	if err != nil {
		return application.Result{}, err
	}
	now := time.Now().UTC()
	var ledger *domain.LedgerData
	if decision.Pending && expired {
		decision.Pending = false
		decision.FailureCode = "REFERENCE_NOT_FOUND"
	}
	switch {
	case decision.Pending:
		if err = wg.PendingReference(now); err != nil {
			return application.Result{}, err
		}
	case decision.FailureCode != "":
		if err = wg.Reject(decision.FailureCode, w.Balance, w.Version, now); err != nil {
			return application.Result{}, err
		}
	default:
		before := w.Balance
		wallet, e := domain.RehydrateWallet(w)
		if e != nil {
			return application.Result{}, e
		}
		switch decision.Direction {
		case domain.DirectionDebit:
			err = wallet.Debit(d.Money, now)
		case domain.DirectionCredit:
			err = wallet.Credit(d.Money, now)
		}
		if err != nil {
			return application.Result{}, err
		}
		w = wallet.Snapshot()
		var referenceID domain.WagerTransactionID
		if reference != nil {
			referenceID = reference.ID
		}
		if err = wg.Process(w.Balance, w.Version, referenceID, now); err != nil {
			return application.Result{}, err
		}
		if decision.Direction != domain.DirectionNone {
			if _, err = tx.Exec(ctx, `UPDATE wallet SET balance_minor=$2,version=$3,updated_at=$4 WHERE id=$1`, w.ID.String(), w.Balance.Minor(), w.Version, w.UpdatedAt); err != nil {
				return application.Result{}, dbError(err)
			}
			entry, e := insertLedger(ctx, tx, wg.Snapshot(), decision.Direction, before, w.Balance, now)
			if e != nil {
				return application.Result{}, dbError(e)
			}
			ledger = &entry
		}
	}
	d = wg.Snapshot()
	if err = persistResult(ctx, tx, d); err != nil {
		return application.Result{}, dbError(err)
	}
	// Pending-reference polling is internal maintenance, not a second transition
	// event. Emit only when first entering the durable wait state.
	if !(d.Status == domain.StatusPendingReference && wasPendingReference) {
		if err = writeEvents(ctx, tx, d, w, ledger, correlation, causation, now); err != nil {
			return application.Result{}, dbError(err)
		}
	}
	return resultOf(d, w, false), nil
}

func persistResult(ctx context.Context, tx pgx.Tx, d domain.WagerData) error {
	var balance, version, currency, ref any
	if d.Status.Terminal() {
		balance = d.ResultBalance.Minor()
		version = d.ResultVersion
		currency = d.ResultBalance.Currency()
	}
	if d.ReferenceID != (domain.WagerTransactionID{}) {
		ref = d.ReferenceID.String()
	}
	_, err := tx.Exec(ctx, `UPDATE wager_transaction SET status=$2,failure_code=$3,result_balance_minor=$4,result_currency=$5,result_wallet_version=$6,reference_transaction_id=$7,updated_at=$8,completed_at=$9,next_reference_attempt_at=CASE WHEN $2='PENDING_REFERENCE' THEN clock_timestamp()+interval '1 second' ELSE NULL END,reference_claimed_by=NULL,reference_claim_token=NULL,reference_claimed_until=NULL WHERE id=$1`, d.ID.String(), string(d.Status), optional(d.FailureCode), balance, currency, version, ref, d.UpdatedAt, d.CompletedAt)
	return err
}

func insertLedger(ctx context.Context, tx pgx.Tx, d domain.WagerData, direction domain.Direction, before, after domain.Money, now time.Time) (domain.LedgerData, error) {
	id, err := allocateID(ctx, tx)
	if err != nil {
		return domain.LedgerData{}, err
	}
	entry, err := domain.NewLedgerEntry(domain.LedgerData{ID: domain.LedgerEntryID(id), WalletID: d.WalletID, TransactionID: d.ID, Direction: direction, Money: d.Money, BalanceBefore: before, BalanceAfter: after, CreatedAt: now})
	if err != nil {
		return domain.LedgerData{}, err
	}
	e := entry.Snapshot()
	// created_at is stamped by PostgreSQL, not by this process. The ledger page
	// orders on it, and replicas do not share a clock: three processes writing
	// three clocks can order two entries against the order the wallet lock
	// actually granted them. clock_timestamp() is one clock for the whole fleet
	// and advances within the transaction, so entries written together still
	// separate. The value is read back so the entry returned to the caller is
	// the row that exists.
	var persisted string
	var stamped time.Time
	err = tx.QueryRow(ctx, `INSERT INTO wallet_ledger_entry(id,wallet_id,transaction_id,direction,amount_minor,currency,balance_before_minor,balance_after_minor,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp()) RETURNING id::text,created_at`, e.ID.String(), e.WalletID.String(), e.TransactionID.String(), string(e.Direction), e.Money.Minor(), e.Money.Currency(), e.BalanceBefore.Minor(), e.BalanceAfter.Minor()).Scan(&persisted, &stamped)
	if err != nil {
		return e, err
	}
	e.CreatedAt = stamped.UTC()
	return e, nil
}
