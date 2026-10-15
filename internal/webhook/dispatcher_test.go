package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

// receiver is a test webhook endpoint that records what it was sent and
// answers with a scripted sequence of status codes.
type receiver struct {
	srv      *httptest.Server
	mu       sync.Mutex
	payloads []Payload
	headers  []http.Header
	statuses []int // consumed in order; last value repeats
	hits     atomic.Int32
}

func newReceiver(t *testing.T, statuses ...int) *receiver {
	t.Helper()
	r := &receiver{statuses: statuses}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		n := int(r.hits.Add(1)) - 1
		body, _ := io.ReadAll(req.Body)
		var p Payload
		_ = json.Unmarshal(body, &p)

		r.mu.Lock()
		r.payloads = append(r.payloads, p)
		r.headers = append(r.headers, req.Header.Clone())
		status := r.statuses[min(n, len(r.statuses)-1)]
		r.mu.Unlock()

		w.WriteHeader(status)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func seedSucceededEvent(t *testing.T, pool *database.Pool, store *Store) *events.Event {
	t.Helper()
	ctx := context.Background()
	es := events.NewStore(pool)
	e, err := es.Insert(ctx, pool, events.Input{
		Type: "payment.created", PaymentID: "pay_1", Amount: 4999, Currency: "EUR", CustomerID: "cus_1",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Mimic the worker: claim, mark succeeded and enqueue in one transaction.
	if _, err := es.ClaimPending(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := es.MarkSucceeded(ctx, tx, e.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueForEvent(ctx, tx, e.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return e
}

func fastCfg() Config {
	return Config{
		Concurrency:    2,
		PollInterval:   20 * time.Millisecond,
		Timeout:        2 * time.Second,
		MaxRetries:     3,
		RetryBaseDelay: 30 * time.Millisecond,
		StaleAfter:     time.Minute,
	}
}

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

func TestDeliverySuccessRecordsAttempt(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()
	rcv := newReceiver(t, http.StatusOK)

	ep, err := store.CreateEndpoint(ctx, EndpointInput{URL: rcv.srv.URL, Secret: "s3cret"})
	if err != nil {
		t.Fatal(err)
	}
	if !ep.HasSecret {
		t.Error("HasSecret should be true")
	}
	e := seedSucceededEvent(t, pool, store)

	d := NewDispatcher(fastCfg(), pool, nil, logging.Discard())
	delivered, err := d.DeliverOne(ctx, logging.Discard())
	if err != nil || !delivered {
		t.Fatalf("DeliverOne = %v, %v", delivered, err)
	}

	// Receiver got a correctly shaped, signed payload.
	rcv.mu.Lock()
	p, h := rcv.payloads[0], rcv.headers[0]
	rcv.mu.Unlock()
	if p.Event == nil || p.Event.ID != e.ID || p.Event.Status != events.StatusSucceeded || p.Attempt != 1 {
		t.Errorf("payload = %+v", p)
	}
	if h.Get(headerEventID) != e.ID.String() || h.Get(headerAttempt) != "1" {
		t.Errorf("headers = %v", h)
	}
	if sig := h.Get(headerSignature); len(sig) != len("sha256=")+64 {
		t.Errorf("signature header = %q", sig)
	}

	// Attempt row records the outcome.
	attempts, err := store.ListAttemptsForEvent(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(attempts))
	}
	a := attempts[0]
	if a.Status != AttemptSucceeded || a.ResponseStatus == nil || *a.ResponseStatus != 200 || a.AttemptedAt == nil {
		t.Errorf("attempt = %+v", a)
	}

	// Nothing left to do.
	if delivered, _ := d.DeliverOne(ctx, logging.Discard()); delivered {
		t.Error("queue should be empty")
	}
}

func TestDeliveryRetriesWithBackoffThenGivesUp(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()
	rcv := newReceiver(t, http.StatusInternalServerError)

	if _, err := store.CreateEndpoint(ctx, EndpointInput{URL: rcv.srv.URL}); err != nil {
		t.Fatal(err)
	}
	e := seedSucceededEvent(t, pool, store)

	cfg := fastCfg()
	cfg.MaxRetries = 2
	d := NewDispatcher(cfg, pool, nil, logging.Discard())
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { d.Run(runCtx); close(done) }()

	// 1 initial + 2 retries = 3 attempts total, all failed, none pending.
	waitFor(t, 10*time.Second, func() bool {
		as, _ := store.ListAttemptsForEvent(ctx, e.ID)
		if len(as) != 3 {
			return false
		}
		for _, a := range as {
			if a.Status != AttemptFailed {
				return false
			}
		}
		return true
	})
	time.Sleep(150 * time.Millisecond) // make sure no 4th attempt appears
	cancel()
	<-done

	if got := rcv.hits.Load(); got != 3 {
		t.Errorf("receiver hit %d times, want 3", got)
	}
	as, _ := store.ListAttemptsForEvent(ctx, e.ID)
	if len(as) != 3 {
		t.Fatalf("attempts = %d, want 3", len(as))
	}
	for i, a := range as {
		if a.AttemptNumber != i+1 || a.ResponseStatus == nil || *a.ResponseStatus != 500 || a.Error == nil {
			t.Errorf("attempt %d = %+v", i+1, a)
		}
	}
	// Exponential spacing: attempt 2 >= 30ms after attempt 1, attempt 3 >= 60ms after 2.
	gap1 := as[1].NextAttemptAt.Sub(as[0].CreatedAt)
	gap2 := as[2].NextAttemptAt.Sub(as[1].NextAttemptAt)
	if gap1 < 30*time.Millisecond || gap2 < 60*time.Millisecond {
		t.Errorf("backoff gaps too short: %s, %s", gap1, gap2)
	}
}

func TestDeliveryEventuallySucceeds(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()
	rcv := newReceiver(t, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusAccepted)

	if _, err := store.CreateEndpoint(ctx, EndpointInput{URL: rcv.srv.URL}); err != nil {
		t.Fatal(err)
	}
	e := seedSucceededEvent(t, pool, store)

	d := NewDispatcher(fastCfg(), pool, nil, logging.Discard())
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { d.Run(runCtx); close(done) }()

	waitFor(t, 10*time.Second, func() bool {
		as, _ := store.ListAttemptsForEvent(ctx, e.ID)
		return len(as) == 3 && as[2].Status == AttemptSucceeded
	})
	cancel()
	<-done

	as, _ := store.ListAttemptsForEvent(ctx, e.ID)
	want := []int{502, 503, 202}
	for i, a := range as {
		if a.ResponseStatus == nil || *a.ResponseStatus != want[i] {
			t.Errorf("attempt %d response_status = %v, want %d", i+1, a.ResponseStatus, want[i])
		}
	}
	if as[0].Status != AttemptFailed || as[1].Status != AttemptFailed || as[2].Status != AttemptSucceeded {
		t.Errorf("statuses = %s %s %s", as[0].Status, as[1].Status, as[2].Status)
	}
}

func TestDeliveryTimeoutIsRecordedAsFailure(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()

	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); slow.Close() })

	if _, err := store.CreateEndpoint(ctx, EndpointInput{URL: slow.URL}); err != nil {
		t.Fatal(err)
	}
	e := seedSucceededEvent(t, pool, store)

	cfg := fastCfg()
	cfg.Timeout = 100 * time.Millisecond
	cfg.MaxRetries = 0
	d := NewDispatcher(cfg, pool, nil, logging.Discard())
	if _, err := d.DeliverOne(ctx, logging.Discard()); err != nil {
		t.Fatal(err)
	}

	as, _ := store.ListAttemptsForEvent(ctx, e.ID)
	if len(as) != 1 || as[0].Status != AttemptFailed || as[0].ResponseStatus != nil || as[0].Error == nil || *as[0].Error != "request timed out" {
		t.Errorf("attempts = %+v", as)
	}
}

func TestFanOutToMultipleEndpointsAndInactiveSkipped(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()
	r1 := newReceiver(t, http.StatusOK)
	r2 := newReceiver(t, http.StatusNoContent)

	if _, err := store.CreateEndpoint(ctx, EndpointInput{URL: r1.srv.URL}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateEndpoint(ctx, EndpointInput{URL: r2.srv.URL}); err != nil {
		t.Fatal(err)
	}
	inactive, err := store.CreateEndpoint(ctx, EndpointInput{URL: "http://127.0.0.1:1/never"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE webhook_endpoints SET active = false WHERE id = $1`, inactive.ID); err != nil {
		t.Fatal(err)
	}

	e := seedSucceededEvent(t, pool, store)

	d := NewDispatcher(fastCfg(), pool, nil, logging.Discard())
	for i := 0; i < 2; i++ {
		if ok, err := d.DeliverOne(ctx, logging.Discard()); err != nil || !ok {
			t.Fatalf("DeliverOne #%d: %v %v", i, ok, err)
		}
	}
	if ok, _ := d.DeliverOne(ctx, logging.Discard()); ok {
		t.Error("inactive endpoint should not have been queued")
	}
	if r1.hits.Load() != 1 || r2.hits.Load() != 1 {
		t.Errorf("hits = %d, %d; want 1, 1", r1.hits.Load(), r2.hits.Load())
	}
	as, _ := store.ListAttemptsForEvent(ctx, e.ID)
	if len(as) != 2 {
		t.Errorf("attempts = %d, want 2", len(as))
	}
}

// TestConcurrentDispatchersDeliverEachAttemptOnce guards against double
// delivery when several dispatcher goroutines race on the queue.
func TestConcurrentDispatchersDeliverEachAttemptOnce(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()
	rcv := newReceiver(t, http.StatusOK)
	if _, err := store.CreateEndpoint(ctx, EndpointInput{URL: rcv.srv.URL}); err != nil {
		t.Fatal(err)
	}

	const n = 40
	ids := make(map[uuid.UUID]bool, n)
	for i := 0; i < n; i++ {
		ids[seedSucceededEvent(t, pool, store).ID] = true
	}

	cfg := fastCfg()
	cfg.Concurrency = 4
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		d := NewDispatcher(cfg, pool, nil, logging.Discard())
		wg.Add(1)
		go func() { defer wg.Done(); d.Run(runCtx) }()
	}
	waitFor(t, 10*time.Second, func() bool { return rcv.hits.Load() >= n })
	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()

	if got := rcv.hits.Load(); got != n {
		t.Errorf("receiver hit %d times, want exactly %d", got, n)
	}
	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	seen := map[uuid.UUID]int{}
	for _, p := range rcv.payloads {
		seen[p.Event.ID]++
	}
	for id := range ids {
		if seen[id] != 1 {
			t.Errorf("event %s delivered %d times", id, seen[id])
		}
	}
}

func TestDeleteEndpointCascadesAndNotFound(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()

	ep, err := store.CreateEndpoint(ctx, EndpointInput{URL: "https://example.com/x"})
	if err != nil {
		t.Fatal(err)
	}
	e := seedSucceededEvent(t, pool, store)
	if as, _ := store.ListAttemptsForEvent(ctx, e.ID); len(as) != 1 {
		t.Fatalf("expected 1 queued attempt, got %d", len(as))
	}

	if err := store.DeleteEndpoint(ctx, ep.ID); err != nil {
		t.Fatal(err)
	}
	if as, _ := store.ListAttemptsForEvent(ctx, e.ID); len(as) != 0 {
		t.Errorf("attempts should cascade on delete, got %d", len(as))
	}
	if err := store.DeleteEndpoint(ctx, ep.ID); err != ErrEndpointNotFound {
		t.Errorf("second delete: %v, want ErrEndpointNotFound", err)
	}
	if eps, _ := store.ListEndpoints(ctx); len(eps) != 0 {
		t.Errorf("endpoints = %d, want 0", len(eps))
	}
}
