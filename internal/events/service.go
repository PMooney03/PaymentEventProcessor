package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/PMooney03/payment-event-processor/internal/database"
	"github.com/PMooney03/payment-event-processor/internal/idempotency"
)

// Service implements the event-ingestion use cases. HTTP handlers call it;
// it never knows about HTTP.
type Service struct {
	pool  *database.Pool
	store *Store
	idem  *idempotency.Store
}

// NewService wires a Service over the given pool.
func NewService(pool *database.Pool) *Service {
	return &Service{pool: pool, store: NewStore(pool), idem: idempotency.NewStore()}
}

// CreateResult is the outcome of Create.
type CreateResult struct {
	Event *Event
	// Replayed is true when the response was served from a previously
	// completed request with the same idempotency key. Callers should return
	// the same status code they would have the first time.
	Replayed bool
}

// Create validates and persists a new pending event.
//
// If idempotencyKey is non-empty the operation is idempotent: a repeated
// request with the same key and payload returns the original event without
// creating a second one, even under concurrent submission. A repeated key
// with a different payload fails with idempotency.ErrPayloadMismatch.
func (s *Service) Create(ctx context.Context, in Input, idempotencyKey string) (*CreateResult, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	if idempotencyKey == "" {
		e, err := s.store.Insert(ctx, s.pool, in)
		if err != nil {
			return nil, fmt.Errorf("create event: %w", err)
		}
		return &CreateResult{Event: e}, nil
	}
	return s.createIdempotent(ctx, in, idempotencyKey)
}

func (s *Service) createIdempotent(ctx context.Context, in Input, key string) (*CreateResult, error) {
	hash, err := idempotency.HashPayload(in)
	if err != nil {
		return nil, err
	}

	var (
		result   *CreateResult
		existing *idempotency.Record
	)
	err = database.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		won, rec, err := s.idem.Reserve(ctx, tx, key, hash)
		if err != nil {
			return err
		}
		if !won {
			existing = rec
			return nil
		}

		// We own the key: create the event and record the response in the
		// same transaction so either both persist or neither does.
		e, err := s.store.Insert(ctx, tx, in)
		if err != nil {
			return err
		}
		body, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("encode event: %w", err)
		}
		if err := s.idem.Complete(ctx, tx, key, e.ID, 201, body); err != nil {
			return err
		}
		result = &CreateResult{Event: e}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create event idempotently: %w", err)
	}
	if result != nil {
		return result, nil
	}

	// Someone else holds this key.
	if existing.RequestHash != hash {
		return nil, idempotency.ErrPayloadMismatch
	}
	if !existing.Completed() {
		return nil, idempotency.ErrInProgress
	}
	var e Event
	if err := json.Unmarshal(existing.ResponseBody, &e); err != nil {
		return nil, fmt.Errorf("decode stored idempotent response: %w", err)
	}
	return &CreateResult{Event: &e, Replayed: true}, nil
}

// Get returns a single event or ErrNotFound.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*Event, error) {
	return s.store.Get(ctx, id)
}

// List returns a page of events matching the filter.
func (s *Service) List(ctx context.Context, f ListFilter) (*Page, error) {
	return s.store.List(ctx, f)
}
