package http

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/webhook"
)

// WebhookService is the slice of the webhook store the handlers need.
type WebhookService interface {
	CreateEndpoint(ctx context.Context, in webhook.EndpointInput) (*webhook.Endpoint, error)
	ListEndpoints(ctx context.Context) ([]webhook.Endpoint, error)
	DeleteEndpoint(ctx context.Context, id uuid.UUID) error
	ListAttemptsForEvent(ctx context.Context, eventID uuid.UUID) ([]webhook.Attempt, error)
}

type webhooksHandler struct {
	svc WebhookService
}

// create handles POST /api/webhooks.
func (h *webhooksHandler) create(w http.ResponseWriter, r *http.Request) {
	var in webhook.EndpointInput
	if err := decodeJSON(w, r, &in); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_json", err.Error(), nil)
		return
	}
	if err := in.Validate(); err != nil {
		var verr *webhook.ValidationError
		if errors.As(err, &verr) {
			writeError(w, r, http.StatusBadRequest, "validation_failed", "one or more fields are invalid", verr.Fields)
			return
		}
		writeInternalError(w, r, err)
		return
	}

	ep, err := h.svc.CreateEndpoint(r.Context(), in)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/webhooks/"+ep.ID.String())
	writeJSON(w, r, http.StatusCreated, ep)
}

// list handles GET /api/webhooks.
func (h *webhooksHandler) list(w http.ResponseWriter, r *http.Request) {
	eps, err := h.svc.ListEndpoints(r.Context())
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, map[string]any{"data": eps})
}

// remove handles DELETE /api/webhooks/{id}.
func (h *webhooksHandler) remove(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "webhook id must be a UUID", nil)
		return
	}
	if err := h.svc.DeleteEndpoint(r.Context(), id); err != nil {
		if errors.Is(err, webhook.ErrEndpointNotFound) {
			writeError(w, r, http.StatusNotFound, "not_found", "webhook endpoint not found", nil)
			return
		}
		writeInternalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// attemptsForEvent handles GET /api/events/{id}/webhook-attempts.
func (h *webhooksHandler) attemptsForEvent(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "event id must be a UUID", nil)
		return
	}
	attempts, err := h.svc.ListAttemptsForEvent(r.Context(), id)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, map[string]any{"data": attempts})
}
