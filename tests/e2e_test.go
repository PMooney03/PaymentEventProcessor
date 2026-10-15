// Package tests contains end-to-end tests that wire the real HTTP API,
// worker and webhook dispatcher together against a PostgreSQL database and
// exercise the system the way a client would. They are skipped unless
// TEST_DATABASE_URL is set.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PMooney03/payment-event-processor/internal/database"
	"github.com/PMooney03/payment-event-processor/internal/database/dbtest"
	"github.com/PMooney03/payment-event-processor/internal/events"
	apihttp "github.com/PMooney03/payment-event-processor/internal/http"
	"github.com/PMooney03/payment-event-processor/internal/logging"
	"github.com/PMooney03/payment-event-processor/internal/webhook"
	"github.com/PMooney03/payment-event-processor/internal/worker"
)

// system is a fully wired application instance on top of a test database.
type system struct {
	t      *testing.T
	pool   *database.Pool
	api    *httptest.Server
	cancel context.CancelFunc
	done   chan struct{}
}

// startSystem boots the API, worker and dispatcher. failureRate controls the
// simulated downstream; maxRetries applies to both events and webhooks.
func startSystem(t *testing.T, failureRate float64, maxRetries int) *system {
	t.Helper()
	pool := dbtest.Pool(t)
	log := logging.Discard()

	eventSvc := events.NewService(pool)
	whStore := webhook.NewStore(pool)

	wrk := worker.New(worker.Config{
		Concurrency:    4,
		PollInterval:   20 * time.Millisecond,
		MaxRetries:     maxRetries,
		RetryBaseDelay: 30 * time.Millisecond,
		StaleAfter:     time.Minute,
	}, pool, worker.Simulated(failureRate, 0, 5*time.Millisecond), webhook.SuccessHook(whStore), log)

	disp := webhook.NewDispatcher(webhook.Config{
		Concurrency:    2,
		PollInterval:   20 * time.Millisecond,
		Timeout:        2 * time.Second,
		MaxRetries:     maxRetries,
		RetryBaseDelay: 30 * time.Millisecond,
		StaleAfter:     time.Minute,
	}, pool, nil, log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); wrk.Run(ctx) }()
		go func() { defer wg.Done(); disp.Run(ctx) }()
		wg.Wait()
		close(done)
	}()

	api := httptest.NewServer(apihttp.NewHandler(apihttp.Deps{
		Logger: log, Events: eventSvc, Webhooks: whStore, DB: pool,
	}))

	s := &system{t: t, pool: pool, api: api, cancel: cancel, done: done}
	t.Cleanup(func() {
		api.Close()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("background loops did not stop")
		}
	})
	return s
}

func (s *system) do(method, path string, body any, headers map[string]string) (int, map[string]any) {
	s.t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			s.t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, s.api.URL+path, rdr)
	if err != nil {
		s.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			s.t.Fatalf("decode %s %s: %v", method, path, err)
		}
	}
	return resp.StatusCode, out
}

func (s *system) waitForStatus(id string, want events.Status, timeout time.Duration) map[string]any {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]any
	for time.Now().Before(deadline) {
		code, body := s.do("GET", "/api/events/"+id, nil, nil)
		if code == http.StatusOK && body["status"] == string(want) {
			return body
		}
		last = body
		time.Sleep(25 * time.Millisecond)
	}
	s.t.Fatalf("event %s did not reach %s in %s; last=%v", id, want, timeout, last)
	return nil
}

func eventBody(paymentID string) map[string]any {
	return map[string]any{
		"type": "payment.created", "payment_id": paymentID,
		"amount": 4999, "currency": "EUR", "customer_id": "cus_e2e",
	}
}

func TestE2E_HappyPathWithWebhook(t *testing.T) {
	s := startSystem(t, 0, 3)

	var received atomic.Int32
	var gotPayload webhook.Payload
	var mu sync.Mutex
	rcv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		_ = json.Unmarshal(b, &gotPayload)
		mu.Unlock()
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer rcv.Close()

	code, ep := s.do("POST", "/api/webhooks", map[string]any{"url": rcv.URL, "secret": "e2e"}, nil)
	if code != http.StatusCreated {
		t.Fatalf("register webhook: %d %v", code, ep)
	}

	code, ev := s.do("POST", "/api/events", eventBody("pay_e2e_1"), map[string]string{"Idempotency-Key": "e2e-1"})
	if code != http.StatusCreated {
		t.Fatalf("create event: %d %v", code, ev)
	}
	id := ev["id"].(string)

	final := s.waitForStatus(id, events.StatusSucceeded, 10*time.Second)
	if final["retry_count"] != float64(0) {
		t.Errorf("retry_count = %v, want 0", final["retry_count"])
	}

	deadline := time.Now().Add(10 * time.Second)
	for received.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if received.Load() != 1 {
		t.Fatalf("webhook received %d times, want 1", received.Load())
	}
	mu.Lock()
	if gotPayload.Event == nil || gotPayload.Event.ID.String() != id || gotPayload.Event.Status != events.StatusSucceeded {
		t.Errorf("webhook payload = %+v", gotPayload)
	}
	mu.Unlock()

	// Attempt audit trail is exposed via the API.
	code, at := s.do("GET", "/api/events/"+id+"/webhook-attempts", nil, nil)
	data, _ := at["data"].([]any)
	if code != http.StatusOK || len(data) != 1 {
		t.Fatalf("attempts: %d %v", code, at)
	}
	first := data[0].(map[string]any)
	if first["status"] != "succeeded" || first["response_status"] != float64(200) {
		t.Errorf("attempt = %v", first)
	}

	// Listing and filtering through the API.
	code, list := s.do("GET", "/api/events?status=succeeded&type=payment.created", nil, nil)
	if d, _ := list["data"].([]any); code != http.StatusOK || len(d) != 1 {
		t.Errorf("list: %d %v", code, list)
	}
}

func TestE2E_ConcurrentIdempotentRequestsOverHTTP(t *testing.T) {
	s := startSystem(t, 0, 3)

	const n = 50
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[string]int{}
		codes   = map[int]int{}
		start   = make(chan struct{})
		hdr     = map[string]string{"Idempotency-Key": "burst-key"}
		payload = eventBody("pay_burst")
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			code, body := s.do("POST", "/api/events", payload, hdr)
			mu.Lock()
			defer mu.Unlock()
			codes[code]++
			if id, ok := body["id"].(string); ok {
				ids[id]++
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(ids) != 1 {
		t.Fatalf("clients observed %d distinct event ids, want 1: %v", len(ids), ids)
	}
	if codes[http.StatusCreated] != 1 {
		t.Errorf("exactly one request should get 201, got codes=%v", codes)
	}
	if codes[http.StatusCreated]+codes[http.StatusOK]+codes[http.StatusConflict] != n {
		t.Errorf("unexpected status codes: %v", codes)
	}

	var count int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("events in database = %d, want 1", count)
	}

	// Reusing the key with a different payload is rejected.
	other := eventBody("pay_other")
	if code, _ := s.do("POST", "/api/events", other, hdr); code != http.StatusUnprocessableEntity {
		t.Errorf("payload mismatch: status = %d, want 422", code)
	}
}

func TestE2E_RetriesThenFailsAndNoWebhookSent(t *testing.T) {
	s := startSystem(t, 0, 2)

	var received atomic.Int32
	rcv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
	}))
	defer rcv.Close()
	if code, _ := s.do("POST", "/api/webhooks", map[string]any{"url": rcv.URL}, nil); code != http.StatusCreated {
		t.Fatal("register webhook")
	}

	// The forced-fail marker makes the simulated downstream fail every time.
	code, ev := s.do("POST", "/api/events", eventBody("pay_fail_e2e"), nil)
	if code != http.StatusCreated {
		t.Fatalf("create: %d %v", code, ev)
	}
	id := ev["id"].(string)

	final := s.waitForStatus(id, events.StatusFailed, 10*time.Second)
	if final["retry_count"] != float64(2) {
		t.Errorf("retry_count = %v, want 2", final["retry_count"])
	}
	if final["last_error"] != worker.ErrSimulatedFailure.Error() {
		t.Errorf("last_error = %v", final["last_error"])
	}

	time.Sleep(200 * time.Millisecond)
	if received.Load() != 0 {
		t.Errorf("failed events must not trigger webhooks, got %d", received.Load())
	}
	code, at := s.do("GET", "/api/events/"+id+"/webhook-attempts", nil, nil)
	if d, _ := at["data"].([]any); code != http.StatusOK || len(d) != 0 {
		t.Errorf("no attempts expected: %d %v", code, at)
	}
}

func TestE2E_ManyEventsAllProcessedExactlyOnce(t *testing.T) {
	s := startSystem(t, 0, 3)

	var delivered sync.Map // event id -> count
	rcv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p webhook.Payload
		_ = json.NewDecoder(r.Body).Decode(&p)
		v, _ := delivered.LoadOrStore(p.Event.ID.String(), new(atomic.Int32))
		v.(*atomic.Int32).Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer rcv.Close()
	if code, _ := s.do("POST", "/api/webhooks", map[string]any{"url": rcv.URL}, nil); code != http.StatusCreated {
		t.Fatal("register webhook")
	}

	const n = 40
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		code, ev := s.do("POST", "/api/events", eventBody(fmt.Sprintf("pay_bulk_%d", i)), nil)
		if code != http.StatusCreated {
			t.Fatalf("create #%d: %d", i, code)
		}
		ids = append(ids, ev["id"].(string))
	}
	for _, id := range ids {
		s.waitForStatus(id, events.StatusSucceeded, 15*time.Second)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		delivered.Range(func(_, _ any) bool { count++; return true })
		if count == n {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	for _, id := range ids {
		v, ok := delivered.Load(id)
		if !ok {
			t.Errorf("event %s never delivered", id)
			continue
		}
		if c := v.(*atomic.Int32).Load(); c != 1 {
			t.Errorf("event %s delivered %d times", id, c)
		}
	}
}

func TestE2E_ReadinessReflectsDatabase(t *testing.T) {
	s := startSystem(t, 0, 3)
	if code, body := s.do("GET", "/ready", nil, nil); code != http.StatusOK || body["status"] != "ready" {
		t.Errorf("/ready = %d %v", code, body)
	}
	if code, body := s.do("GET", "/health", nil, nil); code != http.StatusOK || body["status"] != "ok" {
		t.Errorf("/health = %d %v", code, body)
	}
}
