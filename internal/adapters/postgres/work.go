package postgres

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"jungle/internal/application"
	"jungle/internal/domain"
	"time"
)

// The single statement is a short transaction. No SQL lock spans broker I/O.
func (s *Store) ClaimOutbox(ctx context.Context, owner string, limit int, lease time.Duration) ([]application.OutboxRecord, error) {
	if limit < 1 || limit > application.MaxOutboxBatch || lease <= 0 || owner == "" {
		return nil, &application.Error{Code: "INVALID_INPUT"}
	}
	rows, err := s.pool.Query(ctx, `WITH candidates AS (
 SELECT event_id FROM outbox WHERE published_at IS NULL AND next_attempt_at<=clock_timestamp()
 AND (claimed_until IS NULL OR claimed_until<clock_timestamp()) ORDER BY next_attempt_at,occurred_at,event_id
 LIMIT $1 FOR UPDATE SKIP LOCKED)
 UPDATE outbox o SET claimed_by=$2,claim_token=uuidv7(),claimed_until=clock_timestamp()+$3::bigint*interval '1 millisecond'
 FROM candidates c WHERE o.event_id=c.event_id
 RETURNING o.event_id::text,o.event_type,o.aggregate_id::text,o.claim_token::text,o.payload,o.attempts`, limit, owner, lease.Milliseconds())
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	records := make([]application.OutboxRecord, 0, limit)
	for rows.Next() {
		var r application.OutboxRecord
		if err = rows.Scan(&r.EventID, &r.EventType, &r.AggregateID, &r.ClaimToken, &r.Payload, &r.Attempts); err != nil {
			return nil, dbError(err)
		}
		records = append(records, r)
	}
	return records, rows.Err()
}
func (s *Store) MarkPublished(ctx context.Context, id, token string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE outbox SET published_at=clock_timestamp(),claimed_by=NULL,claim_token=NULL,claimed_until=NULL,last_error=NULL WHERE event_id=$1 AND claim_token=$2 AND published_at IS NULL`, id, token)
	if err != nil {
		return dbError(err)
	}
	if tag.RowsAffected() == 0 {
		return &application.Error{Code: "CLAIM_LOST"}
	}
	return nil
}

// The delay is sent, not the deadline. The claim query compares next_attempt_at
// against clock_timestamp(), so a deadline computed from this process's clock is
// read back against the server's, and every backoff comes out short or long by
// whatever the two disagree about. The same idiom guards the two lease columns.
func (s *Store) RetryOutbox(ctx context.Context, id, token string, delay time.Duration) error {
	tag, err := s.pool.Exec(ctx, `UPDATE outbox SET attempts=attempts+1,next_attempt_at=clock_timestamp()+$3::bigint*interval '1 millisecond',claimed_by=NULL,claim_token=NULL,claimed_until=NULL,last_error='PUBLISH_FAILED' WHERE event_id=$1 AND claim_token=$2 AND published_at IS NULL`, id, token, delay.Milliseconds())
	if err != nil {
		return dbError(err)
	}
	if tag.RowsAffected() == 0 {
		return &application.Error{Code: "CLAIM_LOST"}
	}
	return nil
}
func (s *Store) ClaimReferences(ctx context.Context, owner string, limit int, lease time.Duration) ([]application.ReferenceClaim, error) {
	if limit < 1 || limit > application.MaxOutboxBatch || lease <= 0 || owner == "" {
		return nil, &application.Error{Code: "INVALID_INPUT"}
	}
	rows, err := s.pool.Query(ctx, `WITH candidates AS (
 SELECT id FROM wager_transaction WHERE status='PENDING_REFERENCE' AND next_reference_attempt_at<=clock_timestamp()
 AND (reference_claimed_until IS NULL OR reference_claimed_until<clock_timestamp()) ORDER BY next_reference_attempt_at,created_at,id
 LIMIT $1 FOR UPDATE SKIP LOCKED)
 UPDATE wager_transaction t SET reference_claimed_by=$2,reference_claim_token=uuidv7(),reference_claimed_until=clock_timestamp()+$3::bigint*interval '1 millisecond'
 FROM candidates c WHERE t.id=c.id RETURNING t.id::text,t.wallet_id::text,t.reference_claim_token::text,t.reference_attempts,t.created_at`, limit, owner, lease.Milliseconds())
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	claims := make([]application.ReferenceClaim, 0, limit)
	for rows.Next() {
		var r application.ReferenceClaim
		if err = rows.Scan(&r.TransactionID, &r.WalletID, &r.ClaimToken, &r.Attempts, &r.CreatedAt); err != nil {
			return nil, dbError(err)
		}
		claims = append(claims, r)
	}
	return claims, rows.Err()
}

func (s *Store) RetryReference(ctx context.Context, claim application.ReferenceClaim, maxAttempts int, ttl time.Duration) (application.Result, error) {
	if maxAttempts < 1 || ttl <= 0 {
		return application.Result{}, &application.Error{Code: "INVALID_INPUT"}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.Result{}, dbError(err)
	}
	defer tx.Rollback(ctx)
	w, err := lockWallet(ctx, tx, claim.WalletID)
	if err != nil {
		return application.Result{}, dbError(err)
	}
	d, err := scanWager(tx.QueryRow(ctx, "SELECT "+wagerColumns+" FROM wager_transaction t WHERE id=$1 AND wallet_id=$2 AND reference_claim_token=$3 AND status='PENDING_REFERENCE' AND reference_claimed_until>clock_timestamp() FOR UPDATE", claim.TransactionID, claim.WalletID, claim.ClaimToken))
	if errors.Is(err, pgx.ErrNoRows) {
		return application.Result{}, &application.Error{Code: "CLAIM_LOST"}
	}
	if err != nil {
		return application.Result{}, dbError(err)
	}
	var attempts int
	var correlation string
	var causation *string
	if err = tx.QueryRow(ctx, `UPDATE wager_transaction SET reference_attempts=reference_attempts+1 WHERE id=$1 RETURNING reference_attempts,correlation_id,causation_id`, claim.TransactionID).Scan(&attempts, &correlation, &causation); err != nil {
		return application.Result{}, dbError(err)
	}
	wg, err := domain.RehydrateWager(d)
	if err != nil {
		return application.Result{}, err
	}
	cause := ""
	if causation != nil {
		cause = *causation
	}
	if _, err = tx.Exec(ctx, "SAVEPOINT reference_effect"); err != nil {
		return application.Result{}, dbError(err)
	}
	exhausted := attempts >= maxAttempts || time.Since(d.CreatedAt) >= ttl
	result, err := apply(ctx, tx, w, wg, correlation, cause, exhausted)
	if err != nil {
		var invariant *domain.Error
		if !errors.As(err, &invariant) {
			return application.Result{}, err
		}
		// Infrastructure errors abort/retry the complete transaction. A typed
		// invariant failure is retried durably, with all attempted financial
		// effects rolled back before recording its retry or terminal failure.
		if _, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT reference_effect"); err != nil {
			return application.Result{}, dbError(err)
		}
		if exhausted {
			now := time.Now().UTC()
			if err = wg.Fail("PERMANENT_PROCESSING_FAILURE", w.Balance, w.Version, now); err != nil {
				return application.Result{}, err
			}
			if err = persistResult(ctx, tx, wg.Snapshot()); err != nil {
				return application.Result{}, dbError(err)
			}
			if err = writeEvents(ctx, tx, wg.Snapshot(), w, nil, correlation, cause, now); err != nil {
				return application.Result{}, dbError(err)
			}
			result = resultOf(wg.Snapshot(), w, false)
		} else {
			result = resultOf(d, w, false)
			if _, err = tx.Exec(ctx, `UPDATE wager_transaction SET reference_claimed_by=NULL,reference_claim_token=NULL,reference_claimed_until=NULL WHERE id=$1`, claim.TransactionID); err != nil {
				return application.Result{}, dbError(err)
			}
		}
	}
	if result.Status == "PENDING_REFERENCE" {
		delay := time.Second << min(attempts, 12)
		if delay > time.Hour {
			delay = time.Hour
		}
		if _, err = tx.Exec(ctx, `UPDATE wager_transaction SET next_reference_attempt_at=clock_timestamp()+$2::bigint*interval '1 millisecond' WHERE id=$1`, claim.TransactionID, delay.Milliseconds()); err != nil {
			return application.Result{}, dbError(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return application.Result{}, dbError(err)
	}
	return result, nil
}

func (s *Store) ReleaseClaims(ctx context.Context, owner string) error {
	// Only call after local jobs have stopped. Expired jobs retain at-least-once
	// semantics; clearing a token never permits an old worker to mark a new claim.
	if _, err := s.pool.Exec(ctx, `UPDATE outbox SET claimed_by=NULL,claim_token=NULL,claimed_until=NULL WHERE claimed_by=$1 AND published_at IS NULL`, owner); err != nil {
		return dbError(err)
	}
	_, err := s.pool.Exec(ctx, `UPDATE wager_transaction SET reference_claimed_by=NULL,reference_claim_token=NULL,reference_claimed_until=NULL WHERE reference_claimed_by=$1 AND status='PENDING_REFERENCE'`, owner)
	if err != nil {
		return dbError(err)
	}
	return nil
}
func (s *Store) Backlog(ctx context.Context) (application.Backlog, error) {
	var b application.Backlog
	var milliseconds int64
	err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM outbox WHERE published_at IS NULL),
 (SELECT count(*) FROM wager_transaction WHERE status='PENDING_REFERENCE'),
 COALESCE((SELECT (extract(epoch FROM (clock_timestamp()-min(occurred_at)))*1000)::bigint FROM outbox WHERE published_at IS NULL),0)`).Scan(&b.OutboxCount, &b.ReferenceCount, &milliseconds)
	b.OldestOutboxAge = time.Duration(milliseconds) * time.Millisecond
	return b, err
}
