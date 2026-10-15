package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/events"
	"github.com/PMooney03/payment-event-processor/internal/logging"
)

// fakeEvents is an in-memory EventService.
type fakeEvents struct {
	mu     sync.Mutex
	byID   map[uuid.UUID]*events.Event
	order  []uuid.UUID
	failer error // if set, every call returns this error
}

func newFakeEvents() *fakeEvents {
	return &fakeEvents{byID: map[uuid.UUID]*events.Event{}}
}

func (f *fakeEvents) Create(_ context.Context, in events.Input) (*events.Event, error) {
	if f.failer != nil {
		return nil, f.failer
	}
	if err := in.Validate(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now().UTC()
	e := &events.Event{
		ID: uuid.New(), Type: in.Type, PaymentID: in.PaymentID, Amount: in.Amount,
		Currency: in.Currency, CustomerID: in.CustomerID, Status: events.StatusPending,
		NextAttemptAt: now, CreatedAt: now, UpdatedAt: now,
	}
	f.byID[e.ID] = e
	f.order = append(f.order, e.ID)
	return e, nil
}

func (f *fakeEvents) Get(_ context.Context, id uuid.UUID) (*events.Event, error) {
	if f.failer != nil {
		return nil, f.failer
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.byID[id]
	if !ok {
		return nil, events.ErrNotFound
	}
	return e, nil
}

func (f *fakeEvents) List(_ context.Context, fl events.ListFilter) (*events.Page, error) {
	if f.failer != nil {
		return nil, f.failer
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	page := &events.Page{Events: []events.Event{}}
	for i := len(f.order) - 1; i >= 0; i-- {
		e := f.byID[f.order[i]]
		if fl.Status != "" && e.Status != fl.Status {
			continue
		}
		if fl.Type != "" && e.Type != fl.Type {
			continue
		}
		page.Events = append(page.Events, *e)
	}
	return page, nil
}

type okPinger struct{ err error }

func (p okPinger) Ping(context.Context) error { return p.err }

func newTestServer(t *testing.T, svc EventService, db Pinger) *httptest.Server {
	t.Helper()
	h := NewHandler(Deps{Logger: logging.Discard(), Events: svc, DB: db})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func doJSON(t *testing.T, method, url, body string, headers map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var out map[string]any
	if resp.ContentLength != 0 {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode body: %v", err)
		}
	}
	return resp, out
}

const validBody = `{"type":"payment.created","payment_id":"pay_123","amount":4999,"currency":"EUR","customer_id":"cus_123"}`

func TestCreateEvent(t *testing.T) {
	srv := newTestServer(t, newFakeEvents(), okPinger{})

	resp, body := doJSON(t, "POST", srv.URL+"/api/events", validBody, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%v", resp.StatusCode, body)
	}
	if body["status"] != "pending" || body["payment_id"] != "pay_123" {
		t.Errorf("unexpected body: %v", body)
	}
	id, _ := body["id"].(string)
	if _, err := uuid.Parse(id); err != nil {
		t.Errorf("id is not a uuid: %v", body["id"])
	}
	if loc := resp.Header.Get("Location"); loc != "/api/events/"+id {
		t.Errorf("Location = %q", loc)
	}
	if resp.Header.Get("X-Request-ID") == "" {
		t.Error("missing X-Request-ID header")
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q", ct)
	}
}

func TestCreateEventRejectsBadInput(t *testing.T) {
	srv := newTestServer(t, newFakeEvents(), okPinger{})

	cases := []struct {
		name     string
		body     string
		wantCode string
		field    string
	}{
		{"malformed json", `{"type":`, "invalid_json", ""},
		{"empty body", ``, "invalid_json", ""},
		{"unknown field", `{"type":"payment.created","payment_id":"p","amount":1,"currency":"EUR","customer_id":"c","extra":1}`, "invalid_json", ""},
		{"wrong type for amount", `{"type":"payment.created","payment_id":"p","amount":"lots","currency":"EUR","customer_id":"c"}`, "invalid_json", ""},
		{"validation", `{"type":"payment.created","payment_id":"","amount":-5,"currency":"EURO","customer_id":"c"}`, "validation_failed", "amount"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := doJSON(t, "POST", srv.URL+"/api/events", tc.body, nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%v", resp.StatusCode, body)
			}
			errObj, _ := body["error"].(map[string]any)
			if errObj["code"] != tc.wantCode {
				t.Errorf("code = %v, want %s", errObj["code"], tc.wantCode)
			}
			if tc.field != "" {
				fields, _ := errObj["fields"].(map[string]any)
				if _, ok := fields[tc.field]; !ok {
					t.Errorf("expected field error for %q, got %v", tc.field, fields)
				}
			}
		})
	}
}

func TestCreateEventOversizedBody(t *testing.T) {
	srv := newTestServer(t, newFakeEvents(), okPinger{})
	big := `{"type":"payment.created","payment_id":"` + strings.Repeat("a", maxBodyBytes) + `"}`
	resp, body := doJSON(t, "POST", srv.URL+"/api/events", big, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%v", resp.StatusCode, body)
	}
}

func TestGetEvent(t *testing.T) {
	fake := newFakeEvents()
	srv := newTestServer(t, fake, okPinger{})
	e, _ := fake.Create(context.Background(), events.Input{
		Type: "payment.created", PaymentID: "pay_1", Amount: 100, Currency: "USD", CustomerID: "cus_1",
	})

	resp, body := doJSON(t, "GET", srv.URL+"/api/events/"+e.ID.String(), "", nil)
	if resp.StatusCode != http.StatusOK || body["id"] != e.ID.String() {
		t.Errorf("status=%d body=%v", resp.StatusCode, body)
	}

	resp, _ = doJSON(t, "GET", srv.URL+"/api/events/"+uuid.NewString(), "", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown id: status = %d, want 404", resp.StatusCode)
	}

	resp, _ = doJSON(t, "GET", srv.URL+"/api/events/not-a-uuid", "", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad id: status = %d, want 400", resp.StatusCode)
	}
}

func TestListEvents(t *testing.T) {
	fake := newFakeEvents()
	srv := newTestServer(t, fake, okPinger{})
	for _, typ := range []string{"payment.created", "payment.refunded", "payment.created"} {
		_, _ = fake.Create(context.Background(), events.Input{
			Type: typ, PaymentID: "pay_1", Amount: 100, Currency: "USD", CustomerID: "cus_1",
		})
	}

	resp, body := doJSON(t, "GET", srv.URL+"/api/events", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if data, _ := body["data"].([]any); len(data) != 3 {
		t.Errorf("got %d events, want 3", len(data))
	}

	_, body = doJSON(t, "GET", srv.URL+"/api/events?type=payment.refunded&status=pending", "", nil)
	if data, _ := body["data"].([]any); len(data) != 1 {
		t.Errorf("filtered: got %d events, want 1", len(data))
	}

	for _, q := range []string{"status=done", "limit=0", "limit=abc", "limit=999", "cursor=xyz"} {
		resp, _ = doJSON(t, "GET", srv.URL+"/api/events?"+q, "", nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("query %q: status = %d, want 400", q, resp.StatusCode)
		}
	}
}

func TestInternalErrorsAreOpaque(t *testing.T) {
	fake := newFakeEvents()
	fake.failer = errors.New("connection reset by peer: secret-host:5432")
	srv := newTestServer(t, fake, okPinger{})

	resp, body := doJSON(t, "POST", srv.URL+"/api/events", validBody, nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	raw, _ := json.Marshal(body)
	if strings.Contains(string(raw), "secret-host") {
		t.Errorf("internal error leaked to client: %s", raw)
	}
}

func TestHealthAndReady(t *testing.T) {
	srv := newTestServer(t, newFakeEvents(), okPinger{})
	resp, body := doJSON(t, "GET", srv.URL+"/health", "", nil)
	if resp.StatusCode != http.StatusOK || body["status"] != "ok" {
		t.Errorf("/health: %d %v", resp.StatusCode, body)
	}
	resp, body = doJSON(t, "GET", srv.URL+"/ready", "", nil)
	if resp.StatusCode != http.StatusOK || body["status"] != "ready" {
		t.Errorf("/ready: %d %v", resp.StatusCode, body)
	}

	down := newTestServer(t, newFakeEvents(), okPinger{err: errors.New("db down")})
	resp, body = doJSON(t, "GET", down.URL+"/ready", "", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("/ready with db down: %d %v", resp.StatusCode, body)
	}
}

func TestRequestIDIsHonouredAndSanitised(t *testing.T) {
	srv := newTestServer(t, newFakeEvents(), okPinger{})

	resp, _ := doJSON(t, "GET", srv.URL+"/health", "", map[string]string{"X-Request-ID": "client-abc-123"})
	if got := resp.Header.Get("X-Request-ID"); got != "client-abc-123" {
		t.Errorf("valid client id not echoed: %q", got)
	}

	resp, _ = doJSON(t, "GET", srv.URL+"/health", "", map[string]string{"X-Request-ID": strings.Repeat("x", 100)})
	if got := resp.Header.Get("X-Request-ID"); got == "" || len(got) > 64 {
		t.Errorf("oversized client id should be replaced, got %q", got)
	}
}

func TestUnknownRouteAndMethod(t *testing.T) {
	srv := newTestServer(t, newFakeEvents(), okPinger{})
	resp, body := doJSON(t, "GET", srv.URL+"/nope", "", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown route: %d %v", resp.StatusCode, body)
	}
	resp, _ = doJSON(t, "DELETE", srv.URL+"/api/events", "", nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("wrong method: %d, want 405", resp.StatusCode)
	}
}

func TestPanicIsRecovered(t *testing.T) {
	h := NewHandler(Deps{Logger: logging.Discard(), Events: newFakeEvents(), DB: okPinger{}})
	mux := http.NewServeMux()
	mux.Handle("/", h)
	// Wrap a panicking handler in the same middleware chain used by the router.
	panicky := chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }),
		requestID(logging.Discard()), recoverPanic(), accessLog())

	rec := httptest.NewRecorder()
	panicky.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}
