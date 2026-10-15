// Package webhook delivers succeeded events to registered HTTP endpoints.
//
// Deliveries are queued in the webhook_attempts table by the worker's success
// hook, inside the same transaction that marks the event succeeded. A
// separate Dispatcher claims due attempts with FOR UPDATE SKIP LOCKED, POSTs
// the event, records the HTTP outcome, and schedules a retry with exponential
// backoff when delivery fails.
package webhook

import (
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Endpoint is a registered webhook receiver.
type Endpoint struct {
	ID        uuid.UUID `json:"id"`
	URL       string    `json:"url"`
	HasSecret bool      `json:"has_secret"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`

	// secret is never serialised; it is only used to sign payloads.
	secret *string
}

// EndpointInput is the client payload for registering an endpoint.
type EndpointInput struct {
	URL    string `json:"url"`
	Secret string `json:"secret,omitempty"`
}

// ErrEndpointNotFound is returned when an endpoint does not exist.
var ErrEndpointNotFound = errors.New("webhook endpoint not found")

// ValidationError mirrors events.ValidationError for endpoint input.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for k, v := range e.Fields {
		parts = append(parts, k+": "+v)
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

const (
	maxURLLength    = 2048
	maxSecretLength = 256
)

// Validate checks and normalises the input.
func (in *EndpointInput) Validate() error {
	fields := map[string]string{}

	in.URL = strings.TrimSpace(in.URL)
	switch u, err := url.Parse(in.URL); {
	case in.URL == "":
		fields["url"] = "is required"
	case len(in.URL) > maxURLLength:
		fields["url"] = "is too long"
	case err != nil:
		fields["url"] = "is not a valid URL"
	case u.Scheme != "http" && u.Scheme != "https":
		fields["url"] = "must use http or https"
	case u.Host == "":
		fields["url"] = "must include a host"
	case u.User != nil:
		fields["url"] = "must not include credentials"
	}

	if len(in.Secret) > maxSecretLength {
		fields["secret"] = "is too long"
	}

	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// AttemptStatus is the state of one delivery attempt.
type AttemptStatus string

const (
	AttemptPending    AttemptStatus = "pending"
	AttemptDelivering AttemptStatus = "delivering"
	AttemptSucceeded  AttemptStatus = "succeeded"
	AttemptFailed     AttemptStatus = "failed"
)

// Attempt is one delivery attempt of one event to one endpoint.
type Attempt struct {
	ID             uuid.UUID     `json:"id"`
	EndpointID     uuid.UUID     `json:"endpoint_id"`
	EventID        uuid.UUID     `json:"event_id"`
	AttemptNumber  int           `json:"attempt_number"`
	Status         AttemptStatus `json:"status"`
	ResponseStatus *int          `json:"response_status,omitempty"`
	Error          *string       `json:"error,omitempty"`
	NextAttemptAt  time.Time     `json:"next_attempt_at"`
	AttemptedAt    *time.Time    `json:"attempted_at,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
}
