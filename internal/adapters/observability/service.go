package observability

import (
	"context"
	"jungle/internal/application"
	"log/slog"
	"strconv"
	"time"
)

// Service instruments ports without importing metrics into the domain or SQL.
type Service struct {
	application.Wallets
	application.Wagers
	Metrics *Metrics
	Logger  *slog.Logger
}

// WagerOutcome collapses a result into the small, declared set the duration histogram
// is allowed to carry. A replay is kept apart from the work it skipped, and a
// rejection apart from a debit, because averaging them together hides the only
// number anyone asks for: how long a real BET takes.
func WagerOutcome(r application.Result, err error) string {
	switch {
	case err != nil:
		return "ERROR"
	case r.IdempotentReplay:
		return "REPLAY"
	case r.Status == "":
		return "UNKNOWN"
	default:
		return r.Status
	}
}

func (s *Service) ProcessWager(ctx context.Context, c application.Command, inbox *application.InboxMessage) (application.Result, error) {
	started := time.Now()
	r, err := s.Wagers.ProcessWager(ctx, c, inbox)
	if inbox != nil {
		return r, err
	} // Consumer adds transport-specific observations.
	s.Metrics.ObserveWager("http", c.Kind, WagerOutcome(r, err), time.Since(started))
	if err != nil {
		code := application.Code(err)
		if code == "IDEMPOTENCY_KEY_REUSED" || code == "EXTERNAL_TRANSACTION_ID_REUSED" {
			s.Metrics.Conflicts.WithLabelValues("http").Inc()
		}
		return r, err
	}
	s.Metrics.ObserveResult("http", c.Kind, r.Status, r.FailureCode)
	if r.IdempotentReplay {
		s.Metrics.Duplicates.WithLabelValues("http").Inc()
	}
	s.Logger.Info("wager committed", "correlationId", c.CorrelationID, "transactionId", r.TransactionID, "walletId", c.WalletID, "providerId", c.ProviderID, "status", r.Status)
	return r, nil
}
func (s *Service) Reconcile(ctx context.Context, id string) (application.Reconciliation, error) {
	r, err := s.Wallets.Reconcile(ctx, id)
	if err == nil {
		// Counting both verdicts makes the mismatch counter readable: one alone
		// cannot distinguish "nothing diverged" from "nothing was checked".
		s.Metrics.Reconciliations.WithLabelValues(strconv.FormatBool(r.Consistent)).Inc()
		if !r.Consistent {
			s.Metrics.ReconciliationMismatch.Inc()
			s.Logger.Warn("ledger reconciliation mismatch", "walletId", id)
		}
	}
	return r, err
}
