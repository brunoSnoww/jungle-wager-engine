package postgres

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"jungle/internal/domain"
	"time"
)

func writeEvents(ctx context.Context, tx pgx.Tx, d domain.WagerData, w domain.WalletData, entry *domain.LedgerData, correlation, causation string, now time.Time) error {
	id, err := allocateID(ctx, tx)
	if err != nil {
		return err
	}
	meta := domain.EventMeta{ID: domain.EventID(id), CorrelationID: correlation, CausationID: causation, OccurredAt: now}
	var event domain.Event
	switch d.Status {
	case domain.StatusProcessed:
		event, err = domain.NewWagerProcessed(meta, d)
	case domain.StatusRejected:
		event, err = domain.NewWagerRejected(meta, d)
	case domain.StatusPendingReference:
		event, err = domain.NewWagerPendingReference(meta, d)
	case domain.StatusFailed:
		event, err = domain.NewWagerFailed(meta, d)
	}
	if err != nil {
		return err
	}
	if err = insertEvent(ctx, tx, event, correlation, causation); err != nil {
		return err
	}
	if entry != nil {
		id, err = allocateID(ctx, tx)
		if err != nil {
			return err
		}
		meta.ID = domain.EventID(id)
		event, err = domain.NewWalletBalanceChanged(meta, *entry, w.Version)
		if err != nil {
			return err
		}
		return insertEvent(ctx, tx, event, correlation, causation)
	}
	return nil
}
func insertEvent(ctx context.Context, tx pgx.Tx, event domain.Event, correlation, causation string) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	var id string
	// The column is stamped by PostgreSQL, and it is not the same timestamp as the
	// one inside the payload. The payload carries when the event happened, which
	// the consumer reads and hashes for dedup, so it stays the domain value. The
	// column decides the order the publisher claims and sends in, and FIFO groups
	// by wallet, so an application clock there lets two replicas send one wallet's
	// events in an order the database never committed them in. Here the replicas
	// are containers sharing one VM clock and the two orderings never disagreed
	// across 323,724 events; on separate nodes they would.
	return tx.QueryRow(ctx, `INSERT INTO outbox(event_id,event_type,event_version,aggregate_type,aggregate_id,correlation_id,causation_id,payload,occurred_at,next_attempt_at) VALUES($1,$2,$3,'Wallet',$4,$5,$6,$7,clock_timestamp(),clock_timestamp()) RETURNING event_id::text`, event.ID().String(), event.Type(), event.Version(), event.AggregateID().String(), correlation, optional(causation), payload).Scan(&id)
}
