package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/database"
	"github.com/PMooney03/payment-event-processor/internal/database/dbtest"
	"github.com/PMooney03/payment-event-processor/internal/events"
	"github.com/PMooney03/payment-event-processor/internal/logging"
)

func seedEvents(t *testing.T, pool *database.Pool, n int) []uuid.UUID {
	t.Helper()
	store := events.NewStore(pool)
	ids := make([]uuid.UUID, 0, n)
	for i := 0; i < n; i++ {
		e, err := store.Insert(context.Background(), pool, events.Input{
			Type: "payment.created", PaymentID: "pay_" + uuid.NewString()[:8],
			Amount: 100, Currency: "EUR", CustomerID: "cus_1",
		})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, e.ID)
	}
	return ids
}

func statusCounts(t *testing.T, pool *database.Pool) map[events.Status]int {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT status, count(*) FROM events GROUP BY status`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[events.Status]int{}
	for rows.Next() {
		var s events.Status
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatal(err)
		}
		out[s] = n
	}
	return out
}

// waitFor polls until cond returns true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func fastConfig() Config {
	return Config{
		Concurrency:    4,
		PollInterval:   20 * time.Millisecond,
		MaxRetries:     3,
		RetryBaseDelay: 50 * time.Millisecond,
		StaleAfter:     time.Minute,
	}
}

// TestConcurrentWorkersProcessEachEventExactlyOnce is the central worker
// guarantee: with many workers racing on the same queue, every event is
// handed to the Processor exactly once.
func TestConcurrentWorkersProcessEachEventExactlyOnce(t *testing.T) {
	pool := dbtest.Pool(t)
	const numEvents = 60
	ids := seedEvents(t, pool, numEvents)

	var (
		mu    sync.Mutex
		calls = map[uuid.UUID]int{}
		total atomic.Int64
	)
	processor := func(ctx context.Context, e *events.Event) error {
		mu.Lock()
		calls[e.ID]++
		mu.Unlock()
		total.Add(1)
		time.Sleep(5 * time.Millisecond) // widen the window for overlap
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Two independent Worker instances with 4 goroutines each, to mimic two
	// replicas of the service sharing one database.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		w := New(fastConfig(), pool, processor, nil, logging.Discard())
		wg.Add(1)
		go func() { defer wg.Done(); w.Run(ctx) }()
	}

	waitFor(t, 10*time.Second, func() bool { return total.Load() >= numEvents })
	// Give any stragglers a moment to double-process if they were going to.
	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()

	if got := total.Load(); got != numEvents {
		t.Errorf("processor invoked %d times, want exactly %d", got, numEvents)
	}
	for _, id := range ids {
		if n := calls[id]; n != 1 {
			t.Errorf("event %s processed %d times, want 1", id, n)
		}
	}
	counts := statusCounts(t, pool)
	if counts[events.StatusSucceeded] != numEvents {
		t.Errorf("status counts = %v, want %d succeeded", counts, numEvents)
	}
}

func TestRetryWithBackoffThenPermanentFailure(t *testing.T) {
	pool := dbtest.Pool(t)
	id := seedEvents(t, pool, 1)[0]
	store := events.NewStore(pool)

	var attempts atomic.Int32
	var attemptTimes []time.Time
	var mu sync.Mutex
	alwaysFail := func(ctx context.Context, e *events.Event) error {
		attempts.Add(1)
		mu.Lock()
		attemptTimes = append(attemptTimes, time.Now())
		mu.Unlock()
		return errors.New("downstream exploded")
	}

	cfg := fastConfig()
	cfg.Concurrency = 1
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 100 * time.Millisecond
	w := New(cfg, pool, alwaysFail, nil, logging.Discard())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	// 1 initial attempt + 3 retries = 4 attempts, then failed.
	waitFor(t, 10*time.Second, func() bool {
		e, err := store.Get(context.Background(), id)
		return err == nil && e.Status == events.StatusFailed
	})
	cancel()
	<-done

	if got := attempts.Load(); got != 4 {
		t.Errorf("attempts = %d, want 4 (1 + 3 retries)", got)
	}
	e, _ := store.Get(context.Background(), id)
	if e.RetryCount != 3 {
		t.Errorf("retry_count = %d, want 3", e.RetryCount)
	}
	if e.LastError == nil || *e.LastError != "downstream exploded" {
		t.Errorf("last_error = %v", e.LastError)
	}

	// Gaps between attempts must respect the exponential schedule
	// (100ms, 200ms, 400ms), allowing for poll granularity.
	mu.Lock()
	defer mu.Unlock()
	wantMin := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	for i := 1; i < len(attemptTimes); i++ {
		gap := attemptTimes[i].Sub(attemptTimes[i-1])
		if gap < wantMin[i-1] {
			t.Errorf("gap before attempt %d = %s, want >= %s", i+1, gap, wantMin[i-1])
		}
	}
}

func TestEventuallySucceedsAfterTransientFailures(t *testing.T) {
	pool := dbtest.Pool(t)
	id := seedEvents(t, pool, 1)[0]
	store := events.NewStore(pool)

	var attempts atomic.Int32
	flaky := func(ctx context.Context, e *events.Event) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}

	cfg := fastConfig()
	cfg.RetryBaseDelay = 20 * time.Millisecond
	w := New(cfg, pool, flaky, nil, logging.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	waitFor(t, 10*time.Second, func() bool {
		e, err := store.Get(context.Background(), id)
		return err == nil && e.Status == events.StatusSucceeded
	})
	cancel()
	<-done

	e, _ := store.Get(context.Background(), id)
	if e.RetryCount != 2 || attempts.Load() != 3 {
		t.Errorf("retry_count = %d attempts = %d, want 2 and 3", e.RetryCount, attempts.Load())
	}
	if e.LastError != nil {
		t.Errorf("last_error should be cleared on success, got %q", *e.LastError)
	}
}

func TestMaxRetriesZeroFailsImmediately(t *testing.T) {
	pool := dbtest.Pool(t)
	id := seedEvents(t, pool, 1)[0]
	store := events.NewStore(pool)

	cfg := fastConfig()
	cfg.MaxRetries = 0
	w := New(cfg, pool, func(context.Context, *events.Event) error { return errors.New("no") }, nil, logging.Discard())

	if _, err := w.ProcessOne(context.Background(), logging.Discard()); err != nil {
		t.Fatal(err)
	}
	e, _ := store.Get(context.Background(), id)
	if e.Status != events.StatusFailed || e.RetryCount != 0 {
		t.Errorf("got %s/%d, want failed/0", e.Status, e.RetryCount)
	}
}

func TestSuccessHookRunsAtomicallyWithStatusChange(t *testing.T) {
	pool := dbtest.Pool(t)
	id := seedEvents(t, pool, 1)[0]
	store := events.NewStore(pool)

	// A failing hook must roll back the success and trigger the retry path,
	// so the event never ends up 'succeeded' without its side effect.
	hookErr := errors.New("outbox write failed")
	hook := func(ctx context.Context, tx database.DB, e *events.Event) error { return hookErr }
	ok := func(context.Context, *events.Event) error { return nil }

	cfg := fastConfig()
	w := New(cfg, pool, ok, hook, logging.Discard())
	_, err := w.ProcessOne(context.Background(), logging.Discard())
	if !errors.Is(err, hookErr) {
		t.Fatalf("expected hook error to surface, got %v", err)
	}
	e, _ := store.Get(context.Background(), id)
	if e.Status != events.StatusProcessing {
		// It remains claimed; the stale reaper will return it to the queue.
		t.Errorf("status = %s, want processing (rolled back)", e.Status)
	}

	// Now a working hook: success is recorded exactly once alongside it.
	var hookCalls atomic.Int32
	w2 := New(cfg, pool, ok, func(context.Context, database.DB, *events.Event) error {
		hookCalls.Add(1)
		return nil
	}, logging.Discard())
	if _, err := pool.Exec(context.Background(), `UPDATE events SET status='pending' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := w2.ProcessOne(context.Background(), logging.Discard()); err != nil {
		t.Fatal(err)
	}
	e, _ = store.Get(context.Background(), id)
	if e.Status != events.StatusSucceeded || hookCalls.Load() != 1 {
		t.Errorf("status = %s hookCalls = %d", e.Status, hookCalls.Load())
	}
}

func TestShutdownReleasesInFlightEventWithoutCountingRetry(t *testing.T) {
	pool := dbtest.Pool(t)
	id := seedEvents(t, pool, 1)[0]
	store := events.NewStore(pool)

	started := make(chan struct{})
	slow := func(ctx context.Context, e *events.Event) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	w := New(fastConfig(), pool, slow, nil, logging.Discard())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := w.ProcessOne(ctx, logging.Discard())
		errCh <- err
	}()
	<-started
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	e, _ := store.Get(context.Background(), id)
	if e.Status != events.StatusPending || e.RetryCount != 0 {
		t.Errorf("after shutdown: status=%s retry_count=%d, want pending/0", e.Status, e.RetryCount)
	}
}

func TestPanickingProcessorIsContained(t *testing.T) {
	pool := dbtest.Pool(t)
	id := seedEvents(t, pool, 1)[0]
	store := events.NewStore(pool)

	cfg := fastConfig()
	cfg.MaxRetries = 0
	w := New(cfg, pool, func(context.Context, *events.Event) error { panic("kaboom") }, nil, logging.Discard())
	if _, err := w.ProcessOne(context.Background(), logging.Discard()); err != nil {
		t.Fatal(err)
	}
	e, _ := store.Get(context.Background(), id)
	if e.Status != events.StatusFailed || e.LastError == nil || *e.LastError != "processor panicked: kaboom" {
		t.Errorf("got %+v", e)
	}
}

func TestReclaimStaleReturnsOrphanedEvents(t *testing.T) {
	pool := dbtest.Pool(t)
	id := seedEvents(t, pool, 1)[0]
	store := events.NewStore(pool)

	// Simulate a worker that claimed and then died a long time ago.
	if _, err := store.ClaimPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(),
		`UPDATE events SET updated_at = now() - interval '10 minutes' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	n, err := store.ReclaimStale(context.Background(), 5*time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("reclaimed %d (err=%v), want 1", n, err)
	}
	e, _ := store.Get(context.Background(), id)
	if e.Status != events.StatusPending {
		t.Errorf("status = %s, want pending", e.Status)
	}

	// A freshly claimed event is not stale.
	if _, err := store.ClaimPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	n, err = store.ReclaimStale(context.Background(), 5*time.Minute)
	if err != nil || n != 0 {
		t.Errorf("fresh claim reclaimed %d (err=%v), want 0", n, err)
	}
}

func TestSimulatedProcessor(t *testing.T) {
	ctx := context.Background()
	never := Simulated(0, 0, 0)
	always := Simulated(1, 0, 0)
	ev := &events.Event{PaymentID: "pay_ok"}

	if err := never(ctx, ev); err != nil {
		t.Errorf("failureRate 0 should succeed: %v", err)
	}
	if err := always(ctx, ev); !errors.Is(err, ErrSimulatedFailure) {
		t.Errorf("failureRate 1 should fail: %v", err)
	}
	if err := never(ctx, &events.Event{PaymentID: "pay_FAIL_me"}); !errors.Is(err, ErrSimulatedFailure) {
		t.Errorf("forced-fail marker should fail: %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := Simulated(0, time.Second, time.Second)(cancelled, ev); !errors.Is(err, context.Canceled) {
		t.Errorf("should honour context cancellation: %v", err)
	}
}
