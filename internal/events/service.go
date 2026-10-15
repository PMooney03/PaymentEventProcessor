package events

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/database"
)

// Service implements the event-ingestion use cases. HTTP handlers call it;
// it never knows about HTTP.
type Service struct {
	pool  *database.Pool
	store *Store
}

// NewService wires a Service over the given pool.
func NewService(pool *database.Pool) *Service {
	return &Service{pool: pool, store: NewStore(pool)}
}

// Create validates and persists a new pending event.
func (s *Service) Create(ctx context.Context, in Input) (*Event, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	e, err := s.store.Insert(ctx, s.pool, in)
	if err != nil {
		return nil, fmt.Errorf("create event: %w", err)
	}
	return e, nil
}

// Get returns a single event or ErrNotFound.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*Event, error) {
	return s.store.Get(ctx, id)
}

// List returns a page of events matching the filter.
func (s *Service) List(ctx context.Context, f ListFilter) (*Page, error) {
	return s.store.List(ctx, f)
}
