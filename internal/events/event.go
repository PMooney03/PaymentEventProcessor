// Package events contains the payment event domain model, validation rules,
// persistence and the service that ties them together.
package events

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Status is the processing state of an event.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
)

// ParseStatus validates a user-supplied status string.
func ParseStatus(s string) (Status, bool) {
	switch Status(s) {
	case StatusPending, StatusProcessing, StatusSucceeded, StatusFailed:
		return Status(s), true
	}
	return "", false
}

// Supported event types. Keeping this explicit means a typo in a client
// integration fails fast with a 400 rather than being stored and ignored.
var supportedTypes = map[string]struct{}{
	"payment.created":   {},
	"payment.succeeded": {},
	"payment.failed":    {},
	"payment.refunded":  {},
}

// SupportedTypes returns the sorted list of accepted event types.
func SupportedTypes() []string {
	out := make([]string, 0, len(supportedTypes))
	for t := range supportedTypes {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// Event is a persisted payment event.
type Event struct {
	ID            uuid.UUID `json:"id"`
	Type          string    `json:"type"`
	PaymentID     string    `json:"payment_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	CustomerID    string    `json:"customer_id"`
	Status        Status    `json:"status"`
	RetryCount    int       `json:"retry_count"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	LastError     *string   `json:"last_error,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Input is the client-supplied payload for creating an event.
type Input struct {
	Type       string `json:"type"`
	PaymentID  string `json:"payment_id"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
	CustomerID string `json:"customer_id"`
}

// ErrNotFound is returned when an event does not exist.
var ErrNotFound = errors.New("event not found")

// ValidationError carries per-field messages so the API can return a
// structured 400 response.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	keys := make([]string, 0, len(e.Fields))
	for k := range e.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+": "+e.Fields[k])
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

const (
	maxIDLength = 255
	// maxAmount caps amounts at 10^13 minor units, far above any realistic
	// single payment, while leaving headroom in int64 for arithmetic.
	maxAmount = 10_000_000_000_000
)

var (
	currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)
	// Identifiers are restricted to a conservative character set so they are
	// safe to echo in logs, URLs and webhook payloads.
	identifierRe = regexp.MustCompile(`^[A-Za-z0-9_\-\.]+$`)
)

// Validate normalises and checks the input. It returns a *ValidationError
// listing every failing field, or nil.
func (in *Input) Validate() error {
	fields := map[string]string{}

	in.Type = strings.TrimSpace(in.Type)
	if in.Type == "" {
		fields["type"] = "is required"
	} else if _, ok := supportedTypes[in.Type]; !ok {
		fields["type"] = fmt.Sprintf("must be one of %s", strings.Join(SupportedTypes(), ", "))
	}

	validateID := func(name, value string) {
		switch {
		case value == "":
			fields[name] = "is required"
		case len(value) > maxIDLength:
			fields[name] = fmt.Sprintf("must be at most %d characters", maxIDLength)
		case !identifierRe.MatchString(value):
			fields[name] = "may only contain letters, digits, '_', '-' and '.'"
		}
	}
	in.PaymentID = strings.TrimSpace(in.PaymentID)
	validateID("payment_id", in.PaymentID)
	in.CustomerID = strings.TrimSpace(in.CustomerID)
	validateID("customer_id", in.CustomerID)

	switch {
	case in.Amount <= 0:
		fields["amount"] = "must be a positive integer in minor units (e.g. cents)"
	case in.Amount > maxAmount:
		fields["amount"] = fmt.Sprintf("must be at most %d", int64(maxAmount))
	}

	in.Currency = strings.ToUpper(strings.TrimSpace(in.Currency))
	if !currencyRe.MatchString(in.Currency) {
		fields["currency"] = "must be a 3-letter ISO 4217 code"
	}

	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// ListFilter narrows List results. Zero values mean "no filter".
type ListFilter struct {
	Status Status
	Type   string
	Limit  int
	// Cursor is the ID of the last event on the previous page; results
	// continue strictly after it in (created_at, id) descending order.
	Cursor uuid.UUID
}

const (
	DefaultListLimit = 50
	MaxListLimit     = 200
)

// Page is one page of List results.
type Page struct {
	Events     []Event    `json:"data"`
	NextCursor *uuid.UUID `json:"next_cursor,omitempty"`
}
