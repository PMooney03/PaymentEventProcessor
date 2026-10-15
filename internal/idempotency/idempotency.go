// Package idempotency persists Idempotency-Key records so that retried
// requests are answered with the original result instead of being executed
// again.
//
// The protocol is:
//
//  1. Reserve the key inside the caller's transaction. The primary key on
//     idempotency_keys is the arbiter: of N concurrent requests with the same
//     key, exactly one INSERT succeeds. The others block until the winner's
//     transaction commits or rolls back, then observe the stored row.
//  2. The winner performs the operation and stores the response on the key
//     row with Complete, all in the same transaction, so a crash leaves no
//     half-finished reservation behind.
//  3. Losers compare request hashes. A different payload under the same key
//     is a client bug and gets ErrPayloadMismatch; the same payload gets the
//     stored response replayed.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/database"
)

var (
	// ErrPayloadMismatch means the key was previously used with a different
	// request body.
	ErrPayloadMismatch = errors.New("idempotency key was already used with a different payload")
	// ErrInProgress means another request holding the same key has not yet
	// completed. With transactional reservation this is rare, but it can
	// happen if the original request's transaction is still open.
	ErrInProgress = errors.New("a request with this idempotency key is still in progress")
)

// MaxKeyLength bounds client-supplied keys.
const MaxKeyLength = 255

// Record is a stored idempotency key.
type Record struct {
	Key            string
	RequestHash    string
	EventID        *uuid.UUID
	ResponseStatus *int
	ResponseBody   json.RawMessage
	CreatedAt      time.Time
	CompletedAt    *time.Time
}

// Completed reports whether the original request finished and stored a response.
func (r *Record) Completed() bool {
	return r.CompletedAt != nil && r.ResponseStatus != nil
}

// HashPayload returns a stable fingerprint of v. It is computed from the
// canonical JSON encoding of the validated input rather than the raw request
// bytes, so differences in whitespace or field order do not count as a
// different payload.
func HashPayload(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("hash payload: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Store reads and writes idempotency_keys.
type Store struct{}

// NewStore returns a Store. Every method takes a database.DB explicitly
// because reservation must happen inside the caller's transaction.
func NewStore() *Store {
	return &Store{}
}

// Reserve attempts to claim key for a request with the given payload hash.
// It returns (true, nil, nil) if this caller won the reservation, or
// (false, existing, nil) if the key already exists.
func (s *Store) Reserve(ctx context.Context, db database.DB, key, requestHash string) (bool, *Record, error) {
	tag, err := db.Exec(ctx, `
		INSERT INTO idempotency_keys (key, request_hash)
		VALUES ($1, $2)
		ON CONFLICT (key) DO NOTHING`, key, requestHash)
	if err != nil {
		return false, nil, fmt.Errorf("reserve idempotency key: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil, nil
	}

	existing, err := s.Get(ctx, db, key)
	if err != nil {
		return false, nil, err
	}
	return false, existing, nil
}

// Complete stores the outcome of the winning request on the key row.
func (s *Store) Complete(ctx context.Context, db database.DB, key string, eventID uuid.UUID, status int, body []byte) error {
	tag, err := db.Exec(ctx, `
		UPDATE idempotency_keys
		SET event_id = $2, response_status = $3, response_body = $4, completed_at = now()
		WHERE key = $1 AND completed_at IS NULL`, key, eventID, status, body)
	if err != nil {
		return fmt.Errorf("complete idempotency key: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("complete idempotency key %q: no open reservation", key)
	}
	return nil
}

// Get fetches a key or returns database not-found.
func (s *Store) Get(ctx context.Context, db database.DB, key string) (*Record, error) {
	var r Record
	err := db.QueryRow(ctx, `
		SELECT key, request_hash, event_id, response_status, response_body, created_at, completed_at
		FROM idempotency_keys WHERE key = $1`, key).
		Scan(&r.Key, &r.RequestHash, &r.EventID, &r.ResponseStatus, &r.ResponseBody, &r.CreatedAt, &r.CompletedAt)
	if err != nil {
		return nil, fmt.Errorf("get idempotency key: %w", err)
	}
	return &r, nil
}
