// Package workers coordinates recoverable jobs. Claims are committed before
// network calls, and transport acknowledgement follows the financial commit.
package workers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"jungle/internal/adapters/observability"
	"jungle/internal/adapters/sqs"
	"jungle/internal/application"
	"jungle/internal/domain"
	"jungle/internal/platform/config"
)

type Store interface {
	application.Wagers
	ClaimOutbox(context.Context, string, int, time.Duration) ([]application.OutboxRecord, error)
	MarkPublished(context.Context, string, string) error
	RetryOutbox(context.Context, string, string, time.Duration) error
	ClaimReferences(context.Context, string, int, time.Duration) ([]application.ReferenceClaim, error)
	RetryReference(context.Context, application.ReferenceClaim, int, time.Duration) (application.Result, error)
	Backlog(context.Context) (application.Backlog, error)
	ReleaseClaims(context.Context, string) error
}
type Broker interface {
	Receive(context.Context, int) ([]sqs.Message, error)
	Delete(context.Context, string) error
	Visibility(context.Context, string, time.Duration) error
	Publish(context.Context, string, string, []byte) error
	DLQDepths(context.Context) (map[string]int64, error)
}

// Hooks are injected by tests, never configured by production environment or
// HTTP input. They permit real process termination at durable failure windows.
type Hooks struct {
	AfterCommit  func()
	AfterPublish func()
}
type Engine struct {
	store      Store
	broker     Broker
	config     config.Config
	logger     *slog.Logger
	metrics    *observability.Metrics
	hooks      Hooks
	intake     context.Context
	stopIntake context.CancelFunc
	work       context.Context
	cancelWork context.CancelFunc
	done       chan struct{}
	// receiveCeiling bounds one long poll. Tests lower it to keep the proof fast.
	receiveCeiling time.Duration
	// publishBatch is normalized once here rather than read from config on every
	// poll: a caller that builds Config as a literal and forgets the field would
	// otherwise claim LIMIT 0 and silently publish nothing at all.
	publishBatch int
}

func New(s Store, b Broker, c config.Config, l *slog.Logger, m *observability.Metrics) *Engine {
	batch := c.PublishBatch
	if batch < 1 {
		batch = 1
	}
	return &Engine{store: s, broker: b, config: c, logger: l, metrics: m, receiveCeiling: sqs.LongPollWait + 10*time.Second, publishBatch: batch}
}
func (e *Engine) WithHooks(h Hooks) *Engine { e.hooks = h; return e }
func (e *Engine) Start() {
	e.intake, e.stopIntake = context.WithCancel(context.Background())
	e.work, e.cancelWork = context.WithCancel(context.Background())
	e.done = make(chan struct{})
	go func() {
		defer close(e.done)
		var loops sync.WaitGroup
		for _, run := range []func(){e.consume, e.publish, e.references, e.observe} {
			loops.Add(1)
			go func() { defer loops.Done(); run() }()
		}
		loops.Wait()
	}()
}
func (e *Engine) StopIntake() {
	if e.stopIntake != nil {
		e.stopIntake()
	}
}
func (e *Engine) Stop(ctx context.Context) error {
	e.StopIntake()
	if e.done == nil {
		return nil
	}
	select {
	case <-e.done:
		e.cancelWork()
	case <-ctx.Done():
		e.cancelWork()
		<-e.done
		return ctx.Err()
	}
	cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return e.store.ReleaseClaims(cleanup, e.config.InstanceID)
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func Backoff(attempt int, max time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 20 {
		attempt = 20
	}
	delay := time.Second * time.Duration(1<<attempt)
	if delay > max {
		delay = max
	}
	return delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
}
func (e *Engine) consume() {
	for e.intake.Err() == nil {
		// Long polling holds the call open; without a ceiling a wedged connection
		// would stall intake forever and never surface as a retryable receive.
		ctx, cancel := context.WithTimeout(e.intake, e.receiveCeiling)
		messages, err := e.broker.Receive(ctx, e.config.Concurrency)
		cancel()
		if err != nil {
			if e.intake.Err() != nil {
				return
			}
			e.logger.Warn("SQS receive failed")
			e.metrics.Retries.WithLabelValues("consumer", "receive").Inc()
			if !wait(e.intake, time.Second) {
				return
			}
			continue
		}
		var batch sync.WaitGroup
		for _, message := range messages {
			batch.Add(1)
			go func() { defer batch.Done(); e.handle(message) }()
		}
		batch.Wait()
	}
}

type requestEnvelope struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurredAt"`
	Data       struct {
		ProviderID                     string `json:"providerId"`
		ExternalTransactionID          string `json:"externalTransactionId"`
		IdempotencyKey                 string `json:"idempotencyKey"`
		PlayerID                       string `json:"playerId"`
		WalletID                       string `json:"walletId"`
		RoundID                        string `json:"roundId"`
		GameID                         string `json:"gameId"`
		Kind                           string `json:"kind"`
		ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
		Money                          struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
	} `json:"data"`
}

func Decode(body string, providers []string) (application.Command, *application.InboxMessage, error) {
	invalid := func() (application.Command, *application.InboxMessage, error) {
		return application.Command{}, nil, &application.Error{Code: "INVALID_MESSAGE"}
	}
	if len(body) == 0 || len(body) > 128*1024 {
		return invalid()
	}
	var envelope requestEnvelope
	dec := json.NewDecoder(bytes.NewBufferString(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&envelope); err != nil {
		return invalid()
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return invalid()
	}
	if envelope.MessageID == "" || len(envelope.MessageID) > 200 || envelope.Type != "WagerTransactionRequested" || envelope.OccurredAt.IsZero() {
		return invalid()
	}
	d := envelope.Data
	allowed := false
	for _, provider := range providers {
		if d.ProviderID == provider {
			allowed = true
			break
		}
	}
	if !allowed {
		return invalid()
	}
	money, err := domain.ParseMoney(d.Money.Amount, d.Money.Currency)
	if err != nil {
		return application.Command{}, nil, err
	}
	c := application.Command{ProviderID: d.ProviderID, ExternalTransactionID: d.ExternalTransactionID, IdempotencyKey: d.IdempotencyKey, PlayerID: d.PlayerID, WalletID: d.WalletID, RoundID: d.RoundID, GameID: d.GameID, Kind: d.Kind, Money: money, ReferenceExternalTransactionID: d.ReferenceExternalTransactionID, CorrelationID: envelope.MessageID, CausationID: envelope.MessageID}
	// Both transports use this canonicalization before their financial hash.
	c, err = application.NormalizeCommand(c)
	if err != nil {
		return application.Command{}, nil, err
	}
	hash, err := application.InboxHash(envelope.Type, envelope.OccurredAt, c)
	if err != nil {
		return application.Command{}, nil, err
	}
	return c, &application.InboxMessage{ConsumerName: "wager-transactions-v1", MessageID: envelope.MessageID, PayloadHash: hash}, nil
}
func (e *Engine) handle(message sqs.Message) {
	started := time.Now()
	defer func() { e.metrics.Latency.WithLabelValues("sqs").Observe(time.Since(started).Seconds()) }()
	ctx, cancel := context.WithTimeout(e.work, e.config.ProcessTimeout)
	defer cancel()
	command, inbox, err := Decode(message.Body, e.config.TrustedProviders)
	var result application.Result
	if err == nil {
		wagerStart := time.Now()
		result, err = e.store.ProcessWager(ctx, command, inbox)
		// Measured on the same boundary as the HTTP path, so the two transports
		// are comparable: decoding and broker handling stay outside.
		e.metrics.ObserveWager("sqs", command.Kind, observability.WagerOutcome(result, err), time.Since(wagerStart))
	}
	if err == nil {
		e.metrics.ObserveResult("sqs", command.Kind, result.Status, result.FailureCode)
		if result.IdempotentReplay {
			e.metrics.Duplicates.WithLabelValues("sqs").Inc()
		}
		e.logger.Info("wager committed", "correlationId", command.CorrelationID, "messageId", inbox.MessageID, "transactionId", result.TransactionID, "walletId", command.WalletID, "providerId", command.ProviderID, "status", result.Status)
		if e.hooks.AfterCommit != nil {
			e.hooks.AfterCommit()
		}
		// Deletion has its own bounded context: processing may have consumed its
		// deadline even though the commit succeeded. A failed delete is redelivered.
		ack, stop := context.WithTimeout(e.work, 5*time.Second)
		err = e.broker.Delete(ack, message.Receipt)
		stop()
		if err == nil {
			e.metrics.Messages.WithLabelValues("input", "acknowledged").Inc()
			return
		}
	}
	code := application.Code(err)
	if code == "IDEMPOTENCY_KEY_REUSED" || code == "EXTERNAL_TRANSACTION_ID_REUSED" {
		e.metrics.Conflicts.WithLabelValues("sqs").Inc()
	}
	// Retained is a terminal outcome for this attempt, not an acknowledgement:
	// the message stays for redelivery or redrive.
	e.metrics.Messages.WithLabelValues("input", "retained").Inc()
	e.metrics.Retries.WithLabelValues("consumer", retryReason(code)).Inc()
	e.logger.Warn("message retained for retry or redrive", "messageId", message.ID, "failureCode", code, "receiveCount", message.ReceiveCount)
	cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	delay := Backoff(message.ReceiveCount, 30*time.Second)
	if e.work.Err() != nil {
		delay = 0
	}
	_ = e.broker.Visibility(cleanup, message.Receipt, delay)
}

// retryReason keeps the label bounded: a business classification is named, and
// everything else collapses, so an unexpected error cannot open a new series.
func retryReason(code string) string {
	switch code {
	case "IDEMPOTENCY_KEY_REUSED", "EXTERNAL_TRANSACTION_ID_REUSED", "INBOX_MESSAGE_REUSED", "INVALID_MESSAGE", "NOT_FOUND":
		return strings.ToLower(code)
	default:
		return "infrastructure"
	}
}

func (e *Engine) publish() {
	for e.intake.Err() == nil {
		ctx, cancel := context.WithTimeout(e.intake, 5*time.Second)
		records, err := e.store.ClaimOutbox(ctx, e.config.InstanceID, e.publishBatch, e.config.Lease)
		cancel()
		if err != nil {
			if e.intake.Err() != nil {
				return
			}
			// Without the cause an operator sees a bare warning at 1Hz and a
			// growing backlog, with no way to tell a bad batch size from a
			// connection failure.
			e.logger.Warn("outbox claim failed", "error", err, "batch", e.publishBatch)
			e.metrics.Retries.WithLabelValues("outbox", "claim").Inc()
		}
		var batch sync.WaitGroup
		var published atomic.Int64
		for _, record := range records {
			batch.Add(1)
			go func() {
				defer batch.Done()
				if e.publishOne(record) {
					published.Add(1)
				}
			}()
		}
		batch.Wait()
		// A full batch that fully published means the backlog outran one poll;
		// draining back-to-back is the whole throughput win. Gate it on progress:
		// with the broker down a full batch of failures would otherwise look like
		// progress and walk the entire backlog at full speed, unthrottled.
		if len(records) == e.publishBatch && int(published.Load()) == len(records) {
			continue
		}
		if !wait(e.intake, e.config.PollInterval) {
			return
		}
	}
}

// publishOne reports whether the event reached the broker and was marked, which
// is what the caller treats as progress.
func (e *Engine) publishOne(record application.OutboxRecord) bool {
	ctx, cancel := context.WithTimeout(e.work, e.config.ProcessTimeout)
	defer cancel()
	err := e.broker.Publish(ctx, record.EventID, record.AggregateID, record.Payload)
	if err == nil {
		if e.hooks.AfterPublish != nil {
			e.hooks.AfterPublish()
		}
		err = e.store.MarkPublished(ctx, record.EventID, record.ClaimToken)
		if err == nil {
			e.metrics.OutboxPublished.WithLabelValues(record.EventType, "published").Inc()
			return true
		}
	}
	e.metrics.OutboxPublished.WithLabelValues(record.EventType, "retry").Inc()
	e.metrics.Retries.WithLabelValues("outbox", "publish").Inc()
	e.logger.Warn("outbox publication retry", "eventId", record.EventID)
	cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	_ = e.store.RetryOutbox(cleanup, record.EventID, record.ClaimToken, Backoff(record.Attempts, time.Hour))
	return false
}
func (e *Engine) references() {
	for e.intake.Err() == nil {
		ctx, cancel := context.WithTimeout(e.intake, 5*time.Second)
		claims, err := e.store.ClaimReferences(ctx, e.config.InstanceID, e.config.Concurrency, e.config.Lease)
		cancel()
		if err != nil && e.intake.Err() == nil {
			e.logger.Warn("reference claim failed")
		}
		var batch sync.WaitGroup
		for _, claim := range claims {
			batch.Add(1)
			go func() {
				defer batch.Done()
				ctx, cancel := context.WithTimeout(e.work, e.config.ProcessTimeout)
				defer cancel()
				result, err := e.store.RetryReference(ctx, claim, e.config.ReferenceMaxAttempts, e.config.ReferenceTTL)
				if err != nil {
					e.logger.Warn("reference retry failed", "transactionId", claim.TransactionID)
					e.metrics.ReferenceAttempts.WithLabelValues("error").Inc()
					e.metrics.Retries.WithLabelValues("reference", "resolve").Inc()
					return
				}
				if result.Status == "" {
					// Still waiting on its reference; rescheduled, not resolved.
					e.metrics.ReferenceAttempts.WithLabelValues("deferred").Inc()
					return
				}
				e.metrics.ReferenceAttempts.WithLabelValues("resolved").Inc()
				e.metrics.ObserveResult("reference", "", result.Status, result.FailureCode)
			}()
		}
		batch.Wait()
		if !wait(e.intake, e.config.PollInterval) {
			return
		}
	}
}
func (e *Engine) observe() {
	for e.intake.Err() == nil {
		ctx, cancel := context.WithTimeout(e.intake, 5*time.Second)
		backlog, err := e.store.Backlog(ctx)
		if err == nil {
			e.metrics.OutboxPending.Set(float64(backlog.OutboxCount)) //money:allow-float counts, not money
			e.metrics.OutboxAge.Set(backlog.OldestOutboxAge.Seconds())
			e.metrics.ReferencePending.Set(float64(backlog.ReferenceCount)) //money:allow-float counts, not money
		}
		depths, err := e.broker.DLQDepths(ctx)
		if err == nil {
			for queue, depth := range depths {
				e.metrics.DLQDepth.WithLabelValues(queue).Set(float64(depth)) //money:allow-float counts, not money
			}
		}
		cancel()
		if !wait(e.intake, 10*time.Second) {
			return
		}
	}
}
