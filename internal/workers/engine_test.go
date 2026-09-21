package workers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"jungle/internal/adapters/observability"
	"jungle/internal/adapters/sqs"
	"jungle/internal/application"
	"jungle/internal/platform/config"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const validMessage = `{"messageId":"m-1","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"tx-1","idempotencyKey":"key-1","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round","gameId":"game","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`

func TestDecodeCanonicalMessage(t *testing.T) {
	c, inbox, err := Decode(validMessage, []string{"provider-a"})
	if err != nil {
		t.Fatal(err)
	}
	other, otherInbox, err := Decode(strings.Replace(validMessage, `"25.00"`, `"25"`, 1), []string{"provider-a"})
	if err != nil {
		t.Fatal(err)
	}
	h1, _ := application.PayloadHash(c)
	h2, _ := application.PayloadHash(other)
	if h1 != h2 || inbox.PayloadHash != otherInbox.PayloadHash {
		t.Fatal("equivalent amounts differ")
	}
	other, otherInbox, err = Decode(strings.Replace(validMessage, `"key-1"`, `"key-2"`, 1), []string{"provider-a"})
	if err != nil {
		t.Fatal(err)
	}
	h2, _ = application.PayloadHash(other)
	if h1 != h2 || inbox.PayloadHash == otherInbox.PayloadHash {
		t.Fatal("financial and inbox hash semantics conflated")
	}
	for _, body := range []string{validMessage + `{}`, strings.Replace(validMessage, `"messageId":`, `"unexpected":`, 1), strings.Replace(validMessage, `"BET"`, `"OPENING"`, 1), strings.Replace(validMessage, `"25.00"`, `25.0`, 1), strings.Replace(validMessage, `"25.00"`, `"-1"`, 1)} {
		if _, _, err := Decode(body, []string{"provider-a"}); err == nil {
			t.Fatalf("accepted malformed message: %s", body)
		}
	}
	if _, _, err := Decode(validMessage, []string{"provider-b"}); err == nil {
		t.Fatal("untrusted provider accepted")
	}
}

type fakeStore struct {
	Store
	process         func(context.Context, application.Command, *application.InboxMessage) (application.Result, error)
	claimOutbox     func(context.Context, string, int, time.Duration) ([]application.OutboxRecord, error)
	marked, retried atomic.Int32
}

func (s *fakeStore) ProcessWager(ctx context.Context, c application.Command, i *application.InboxMessage) (application.Result, error) {
	return s.process(ctx, c, i)
}
func (s *fakeStore) MarkPublished(context.Context, string, string) error { s.marked.Add(1); return nil }
func (s *fakeStore) RetryOutbox(context.Context, string, string, time.Duration) error {
	s.retried.Add(1)
	return nil
}
func (s *fakeStore) ClaimOutbox(ctx context.Context, owner string, limit int, lease time.Duration) ([]application.OutboxRecord, error) {
	if s.claimOutbox != nil {
		return s.claimOutbox(ctx, owner, limit, lease)
	}
	return nil, nil
}
func (s *fakeStore) ClaimReferences(context.Context, string, int, time.Duration) ([]application.ReferenceClaim, error) {
	return nil, nil
}
func (s *fakeStore) Backlog(context.Context) (application.Backlog, error) {
	return application.Backlog{}, nil
}
func (s *fakeStore) ReleaseClaims(context.Context, string) error { return nil }

type fakeBroker struct {
	Broker
	receive                     func(context.Context, int) ([]sqs.Message, error)
	deleted, visible, published atomic.Int32
	publishErr                  error
}

func (b *fakeBroker) Receive(ctx context.Context, n int) ([]sqs.Message, error) {
	return b.receive(ctx, n)
}
func (b *fakeBroker) Delete(context.Context, string) error { b.deleted.Add(1); return nil }
func (b *fakeBroker) Visibility(context.Context, string, time.Duration) error {
	b.visible.Add(1)
	return nil
}
func (b *fakeBroker) Publish(context.Context, string, string, []byte) error {
	b.published.Add(1)
	return b.publishErr
}
func (b *fakeBroker) DLQDepths(context.Context) (map[string]int64, error) {
	return map[string]int64{"input": 0, "output": 0}, nil
}
func engineFor(s *fakeStore, b *fakeBroker) *Engine {
	e := New(s, b, config.Config{InstanceID: "test", Concurrency: 1, PublishBatch: 10, ProcessTimeout: time.Second, Lease: time.Minute, PollInterval: time.Millisecond, TrustedProviders: []string{"provider-a"}}, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.New())
	e.work = context.Background()
	return e
}
func TestConsumerAcknowledgesOnlyDurableSuccess(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed", true: "rolledback"}[failure], func(t *testing.T) {
			b := &fakeBroker{}
			s := &fakeStore{process: func(context.Context, application.Command, *application.InboxMessage) (application.Result, error) {
				if b.deleted.Load() != 0 {
					t.Fatal("ack before commit")
				}
				if failure {
					return application.Result{}, errors.New("rollback")
				}
				return application.Result{Status: "REJECTED"}, nil
			}}
			e := engineFor(s, b)
			e.hooks.AfterCommit = func() {
				if b.deleted.Load() != 0 {
					t.Fatal("ack precedes after-commit hook")
				}
			}
			e.handle(sqs.Message{Body: validMessage, Receipt: "receipt", ReceiveCount: 2})
			if failure && (b.deleted.Load() != 0 || b.visible.Load() != 1) {
				t.Fatal("failed transaction acknowledged")
			}
			if !failure && b.deleted.Load() != 1 {
				t.Fatal("committed rejection not acknowledged")
			}
		})
	}
}
func TestPublisherRetainsStableIdentityAndRetries(t *testing.T) {
	s := &fakeStore{}
	b := &fakeBroker{publishErr: errors.New("broker unavailable")}
	e := engineFor(s, b)
	record := application.OutboxRecord{EventID: "stable", AggregateID: "wallet", ClaimToken: "claim", Payload: []byte(`{}`)}
	if e.publishOne(record) {
		t.Fatal("failed send reported as progress")
	}
	if s.marked.Load() != 0 || s.retried.Load() != 1 {
		t.Fatal("failed send marked published")
	}
	b.publishErr = nil
	e.hooks.AfterPublish = func() {
		if s.marked.Load() != 0 {
			t.Fatal("marked before send completed")
		}
	}
	if !e.publishOne(record) {
		t.Fatal("successful send not reported as progress")
	}
	if s.marked.Load() != 1 {
		t.Fatal("successful send not marked")
	}
}
func TestGracefulStopDoesNotCancelInflightWork(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var received atomic.Bool
	s := &fakeStore{process: func(ctx context.Context, _ application.Command, _ *application.InboxMessage) (application.Result, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return application.Result{}, ctx.Err()
		case <-release:
			return application.Result{Status: "PROCESSED"}, nil
		}
	}}
	b := &fakeBroker{receive: func(ctx context.Context, _ int) ([]sqs.Message, error) {
		if received.CompareAndSwap(false, true) {
			return []sqs.Message{{Body: validMessage}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	e := engineFor(s, b)
	e.Start()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("work not started")
	}
	stopped := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { stopped <- e.Stop(ctx) }()
	select {
	case <-stopped:
		t.Fatal("stop returned before drain")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if b.deleted.Load() != 1 {
		t.Fatal("inflight processing canceled instead of drained")
	}
}
func TestNewNormalizesPublishBatch(t *testing.T) {
	// A caller that builds Config as a literal and forgets the field would claim
	// LIMIT 0 and publish nothing at all, silently and forever.
	e := New(&fakeStore{}, &fakeBroker{}, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.New())
	if e.publishBatch < 1 {
		t.Fatalf("zero PublishBatch normalized to %d: the publisher would claim LIMIT 0", e.publishBatch)
	}
}

func TestFailedBatchDoesNotCountAsDrainProgress(t *testing.T) {
	const batch = 3
	var claims atomic.Int32
	s := &fakeStore{}
	s.claimOutbox = func(context.Context, string, int, time.Duration) ([]application.OutboxRecord, error) {
		claims.Add(1)
		return records(batch), nil
	}
	// A broker that always fails: every record is claimed and none is published.
	b := &fakeBroker{publishErr: errors.New("broker down"), receive: func(ctx context.Context, _ int) ([]sqs.Message, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	e := engineFor(s, b)
	e.publishBatch = batch
	// With the brake removed a full batch of failures would look like progress
	// and walk the whole backlog at full speed. Pacing must survive an outage.
	e.config.PollInterval = time.Hour
	e.receiveCeiling = 50 * time.Millisecond
	e.Start()
	time.Sleep(300 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if got := claims.Load(); got != 1 {
		t.Fatalf("publisher claimed %d times while the broker was down: it ignored its own backoff", got)
	}
}

func TestEmptyOutboxClaimDoesNotBusyLoop(t *testing.T) {
	var claims atomic.Int32
	s := &fakeStore{}
	s.claimOutbox = func(context.Context, string, int, time.Duration) ([]application.OutboxRecord, error) {
		claims.Add(1)
		return nil, nil
	}
	b := &fakeBroker{receive: func(ctx context.Context, _ int) ([]sqs.Message, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	e := engineFor(s, b)
	// An idle outbox returns zero records. Zero must not be mistaken for a full
	// batch, or the publisher spins a core instead of pacing.
	// The window must span several intervals: with only one expected poll a
	// publisher that exited after its first claim is indistinguishable from a
	// healthy one that simply had not polled again yet.
	e.config.PollInterval = 100 * time.Millisecond
	e.receiveCeiling = 20 * time.Millisecond
	e.Start()
	time.Sleep(450 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// Healthy is roughly one claim per interval. One means the loop exited; a
	// spinning loop reaches the hundreds.
	if got := claims.Load(); got < 2 || got > 12 {
		t.Fatalf("idle publisher polled %d times across ~4 intervals: want steady pacing, not a dead loop or a spin", got)
	}
}

func TestBackoffBounded(t *testing.T) {
	for _, attempt := range []int{-1, 0, 1, 12, 100} {
		d := Backoff(attempt, time.Minute)
		if d < 0 || d > time.Minute {
			t.Fatalf("invalid backoff %v", d)
		}
	}
}

func TestReceiveIsBoundedSoWedgedPollDoesNotStallIntake(t *testing.T) {
	blocked := make(chan context.Context, 2)
	b := &fakeBroker{receive: func(ctx context.Context, _ int) ([]sqs.Message, error) {
		select {
		case blocked <- ctx:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	e := engineFor(&fakeStore{}, b)
	// A ceiling below the real long poll proves the bound comes from the caller,
	// not from the broker returning on its own.
	e.receiveCeiling = 20 * time.Millisecond
	e.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	var first context.Context
	select {
	case first = <-blocked:
	case <-time.After(time.Second):
		t.Fatal("receive never started")
	}
	if _, ok := first.Deadline(); !ok {
		t.Fatal("receive context has no deadline")
	}
	select {
	case <-first.Done():
	case <-time.After(sqs.LongPollWait):
		t.Fatal("wedged receive was never cut off")
	}
	// Intake must recover into a fresh poll rather than exiting the loop.
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not resume after bounded receive")
	}
}

func TestStopLeavesNoWorkerGoroutines(t *testing.T) {
	b := &fakeBroker{receive: func(ctx context.Context, _ int) ([]sqs.Message, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	e := engineFor(&fakeStore{}, b)
	e.receiveCeiling = 50 * time.Millisecond
	e.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// The claim is that Stop returns only once every loop has exited, so this is
	// sampled immediately: waiting for the count to settle would also pass for a
	// Stop that returned early and let the loops drain behind its back.
	if running := workerGoroutines(); running != 0 {
		t.Fatalf("Stop returned with %d worker goroutines still running:\n%s", running, stacks())
	}
}

// workerGoroutines counts only this package's loops, so unrelated runtime or
// test goroutines cannot mask a leak or invent one.
func workerGoroutines() int {
	return strings.Count(stacks(), "jungle/internal/workers.(*Engine).")
}

func stacks() string {
	buf := make([]byte, 1<<20)
	return string(buf[:runtime.Stack(buf, true)])
}

func TestPublisherDrainsBacklogWithoutWaitingOutThePollInterval(t *testing.T) {
	const batch = 3
	var claims atomic.Int32
	s := &fakeStore{}
	s.claimOutbox = func(_ context.Context, _ string, limit int, _ time.Duration) ([]application.OutboxRecord, error) {
		if limit != batch {
			t.Errorf("publisher claimed %d, want the configured batch %d", limit, batch)
		}
		// Two saturated batches, then a short one that ends the drain.
		switch claims.Add(1) {
		case 1, 2:
			return records(limit), nil
		default:
			return records(limit - 1), nil
		}
	}
	b := &fakeBroker{receive: func(ctx context.Context, _ int) ([]sqs.Message, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	e := engineFor(s, b)
	e.publishBatch = batch
	// A poll interval far longer than the test: reaching the third claim at all
	// proves full batches did not sleep between them.
	e.config.PollInterval = time.Hour
	e.receiveCeiling = 50 * time.Millisecond
	e.Start()
	deadline := time.Now().Add(2 * time.Second)
	for claims.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if got := claims.Load(); got < 3 {
		t.Fatalf("publisher slept between full batches: only %d claims in 2s", got)
	}
	if published := b.published.Load(); published != int32(batch*2+batch-1) {
		t.Fatalf("published %d events, want %d", published, batch*2+batch-1)
	}
}

func records(n int) []application.OutboxRecord {
	out := make([]application.OutboxRecord, 0, n)
	for i := range n {
		out = append(out, application.OutboxRecord{EventID: fmt.Sprintf("event-%d", i), AggregateID: "wallet", ClaimToken: "claim", Payload: []byte(`{}`)})
	}
	return out
}

// Any error code must collapse rather than open a metric series, or a novel
// failure becomes a cardinality incident on top of whatever it already was.
func TestRetryReasonStaysBounded(t *testing.T) {
	for _, c := range []struct{ code, want string }{
		{"IDEMPOTENCY_KEY_REUSED", "idempotency_key_reused"},
		{"INVALID_MESSAGE", "invalid_message"},
		{"SOMETHING_NOBODY_PLANNED", "infrastructure"},
		{"", "infrastructure"},
	} {
		if got := retryReason(c.code); got != c.want {
			t.Fatalf("retryReason(%q) = %q, want %q", c.code, got, c.want)
		}
	}
}
