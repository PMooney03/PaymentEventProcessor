package http

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/webhook"
)

func TestWebhookEndpointLifecycle(t *testing.T) {
	wh := newFakeWebhooks()
	srv := newTestServerWith(t, newFakeEvents(), wh, okPinger{})

	// Create with a secret: the secret must never be echoed back.
	resp, body := doJSON(t, "POST", srv.URL+"/api/webhooks", `{"url":"https://example.com/hook","secret":"shh"}`, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: status = %d body=%v", resp.StatusCode, body)
	}
	if body["has_secret"] != true || body["url"] != "https://example.com/hook" {
		t.Errorf("body = %v", body)
	}
	if _, leaked := body["secret"]; leaked {
		t.Error("secret must not be returned")
	}
	id, _ := body["id"].(string)
	if resp.Header.Get("Location") != "/api/webhooks/"+id {
		t.Errorf("Location = %q", resp.Header.Get("Location"))
	}

	// List.
	resp, body = doJSON(t, "GET", srv.URL+"/api/webhooks", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list: %d", resp.StatusCode)
	}
	if data, _ := body["data"].([]any); len(data) != 1 {
		t.Errorf("list: got %d endpoints, want 1", len(data))
	}

	// Delete, then delete again.
	resp, _ = doJSON(t, "DELETE", srv.URL+"/api/webhooks/"+id, "", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete: status = %d, want 204", resp.StatusCode)
	}
	resp, _ = doJSON(t, "DELETE", srv.URL+"/api/webhooks/"+id, "", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("second delete: status = %d, want 404", resp.StatusCode)
	}
	resp, _ = doJSON(t, "DELETE", srv.URL+"/api/webhooks/nope", "", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad id: status = %d, want 400", resp.StatusCode)
	}
}

func TestWebhookEndpointValidation(t *testing.T) {
	srv := newTestServer(t, newFakeEvents(), okPinger{})
	for _, body := range []string{
		`{"url":""}`,
		`{"url":"ftp://x.com"}`,
		`{"url":"https://u:p@x.com"}`,
		`{"url":"https://x.com","bogus":1}`,
		`not json`,
	} {
		resp, out := doJSON(t, "POST", srv.URL+"/api/webhooks", body, nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d body=%v", body, resp.StatusCode, out)
		}
	}
}

func TestListWebhookAttemptsForEvent(t *testing.T) {
	wh := newFakeWebhooks()
	srv := newTestServerWith(t, newFakeEvents(), wh, okPinger{})
	eventID := uuid.New()
	status := 500
	errText := "endpoint returned HTTP 500"
	wh.attempts[eventID] = []webhook.Attempt{
		{ID: uuid.New(), EventID: eventID, AttemptNumber: 1, Status: webhook.AttemptFailed, ResponseStatus: &status, Error: &errText, CreatedAt: time.Now()},
		{ID: uuid.New(), EventID: eventID, AttemptNumber: 2, Status: webhook.AttemptPending, CreatedAt: time.Now()},
	}

	resp, body := doJSON(t, "GET", srv.URL+"/api/events/"+eventID.String()+"/webhook-attempts", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	data, _ := body["data"].([]any)
	if len(data) != 2 {
		t.Fatalf("got %d attempts, want 2", len(data))
	}
	first, _ := data[0].(map[string]any)
	if first["status"] != "failed" || first["response_status"] != float64(500) || first["error"] != errText {
		t.Errorf("first attempt = %v", first)
	}

	resp, body = doJSON(t, "GET", srv.URL+"/api/events/"+uuid.NewString()+"/webhook-attempts", "", nil)
	if data, _ := body["data"].([]any); resp.StatusCode != http.StatusOK || len(data) != 0 {
		t.Errorf("unknown event should return empty list: %d %v", resp.StatusCode, body)
	}
}
