package http

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/events"
)

// EventService is the slice of the events service the handlers need. It is
// an interface so handler tests can run against an in-memory fake.
type EventService interface {
	Create(ctx context.Context, in events.Input) (*events.Event, error)
	Get(ctx context.Context, id uuid.UUID) (*events.Event, error)
	List(ctx context.Context, f events.ListFilter) (*events.Page, error)
}

type eventsHandler struct {
	svc EventService
}

// create handles POST /api/events.
func (h *eventsHandler) create(w http.ResponseWriter, r *http.Request) {
	var in events.Input
	if err := decodeJSON(w, r, &in); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_json", err.Error(), nil)
		return
	}

	e, err := h.svc.Create(r.Context(), in)
	if err != nil {
		var verr *events.ValidationError
		if errors.As(err, &verr) {
			writeError(w, r, http.StatusBadRequest, "validation_failed", "one or more fields are invalid", verr.Fields)
			return
		}
		writeInternalError(w, r, err)
		return
	}

	w.Header().Set("Location", "/api/events/"+e.ID.String())
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
