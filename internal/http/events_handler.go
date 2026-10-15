package http

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/events"
	"github.com/PMooney03/payment-event-processor/internal/idempotency"
	"github.com/PMooney03/payment-event-processor/internal/logging"
)

// EventService is the slice of the events service the handlers need. It is
// an interface so handler tests can run against an in-memory fake.
type EventService interface {
	Create(ctx context.Context, in events.Input, idempotencyKey string) (*events.CreateResult, error)
	Get(ctx context.Context, id uuid.UUID) (*events.Event, error)
	List(ctx context.Context, f events.ListFilter) (*events.Page, error)
}

const idempotencyKeyHeader = "Idempotency-Key"

type eventsHandler struct {
	svc EventService
}

// create handles POST /api/events.
//
// Responses:
//
//	201  event created
//	200  replayed from a previous request with the same Idempotency-Key
//	400  malformed JSON or validation failure
//	409  Idempotency-Key is still being processed by a concurrent request
//	422  Idempotency-Key reused with a different payload
func (h *eventsHandler) create(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get(idempotencyKeyHeader)
	if len(key) > idempotency.MaxKeyLength {
		writeError(w, r, http.StatusBadRequest, "invalid_idempotency_key",
			"Idempotency-Key must be at most "+strconv.Itoa(idempotency.MaxKeyLength)+" characters", nil)
		return
	}

	var in events.Input
	if err := decodeJSON(w, r, &in); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_json", err.Error(), nil)
		return
	}

	res, err := h.svc.Create(r.Context(), in, key)
	if err != nil {
		var verr *events.ValidationError
		switch {
		case errors.As(err, &verr):
			writeError(w, r, http.StatusBadRequest, "validation_failed", "one or more fields are invalid", verr.Fields)
		case errors.Is(err, idempotency.ErrPayloadMismatch):
			writeError(w, r, http.StatusUnprocessableEntity, "idempotency_key_reused",
				"this Idempotency-Key was already used with a different payload", nil)
		case errors.Is(err, idempotency.ErrInProgress):
			w.Header().Set("Retry-After", "1")
			writeError(w, r, http.StatusConflict, "idempotency_key_in_progress",
				"a request with this Idempotency-Key is still being processed; retry shortly", nil)
		default:
			writeInternalError(w, r, err)
		}
		return
	}

	e := res.Event
	logging.FromContext(r.Context()).Info("event accepted",
		"event_id", e.ID, "payment_id", e.PaymentID, "type", e.Type, "replayed", res.Replayed)

	w.Header().Set("Location", "/api/events/"+e.ID.String())
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, r, http.StatusOK, e)
		return
	}
	writeJSON(w, r, http.StatusCreated, e)
}

// get handles GET /api/events/{id}.
func (h *eventsHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "event id must be a UUID", nil)
		return
	}

	e, err := h.svc.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, events.ErrNotFound) {
			writeError(w, r, http.StatusNotFound, "not_found", "event not found", nil)
			return
		}
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, e)
}

// list handles GET /api/events?status=&type=&limit=&cursor=.
func (h *eventsHandler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f events.ListFilter
	fields := map[string]string{}

	if s := q.Get("status"); s != "" {
		st, ok := events.ParseStatus(s)
		if !ok {
			fields["status"] = "must be one of pending, processing, succeeded, failed"
		}
		f.Status = st
	}
	f.Type = q.Get("type")

	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > events.MaxListLimit {
			fields["limit"] = "must be an integer between 1 and " + strconv.Itoa(events.MaxListLimit)
		}
		f.Limit = n
	}
	if c := q.Get("cursor"); c != "" {
		id, err := uuid.Parse(c)
		if err != nil {
			fields["cursor"] = "must be an event id"
		}
		f.Cursor = id
	}
	if len(fields) > 0 {
		writeError(w, r, http.StatusBadRequest, "validation_failed", "one or more query parameters are invalid", fields)
		return
	}

	page, err := h.svc.List(r.Context(), f)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, page)
}
