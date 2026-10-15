package webhook

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/PMooney03/payment-event-processor/internal/database"
)

// Store persists webhook endpoints and delivery attempts.
type Store struct {
	pool *database.Pool
}

// NewStore returns a Store backed by pool.
func NewStore(pool *database.Pool) *Store {
	return &Store{pool: pool}
}

// ---- Endpoints -------------------------------------------------------------

const endpointColumns = `id, url, secret, active, created_at`

func scanEndpoint(row pgx.Row) (*Endpoint, error) {
	var ep Endpoint
	if err := row.Scan(&ep.ID, &ep.URL, &ep.secret, &ep.Active, &ep.CreatedAt); err != nil {
		return nil, err
	}
	ep.HasSecret = ep.secret != nil && *ep.secret != ""
	return &ep, nil
}

// CreateEndpoint registers a new active endpoint.
func (s *Store) CreateEndpoint(ctx context.Context, in EndpointInput) (*Endpoint, error) {
	var secret *string
	if in.Secret != "" {
		secret = &in.Secret
	}
	ep, err := scanEndpoint(s.pool.QueryRow(ctx, `
		INSERT INTO webhook_endpoints (id, url, secret)
		VALUES ($1, $2, $3)
		RETURNING `+endpointColumns, uuid.New(), in.URL, secret))
	if err != nil {
		return nil, fmt.Errorf("create webhook endpoint: %w", err)
	}
	return ep, nil
}

// ListEndpoints returns all endpoints, newest first.
func (s *Store) ListEndpoints(ctx context.Context) ([]Endpoint, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+endpointColumns+` FROM webhook_endpoints ORDER BY created_at DESC, id DESC`)
	if err != nil {
		return nil, fmt.Errorf("list webhook endpoints: %w", err)
	}
	defer rows.Close()

	out := []Endpoint{}
	for rows.Next() {
		ep, err := scanEndpoint(rows)
		if err != nil {
			return nil, fmt.Errorf("scan webhook endpoint: %w", err)
		}
		out = append(out, *ep)
	}
	return out, rows.Err()
}

// DeleteEndpoint removes an endpoint and (via ON DELETE CASCADE) its attempts.
func (s *Store) DeleteEndpoint(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM webhook_endpoints WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete webhook endpoint: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrEndpointNotFound
	}
	return nil
}

// ---- Attempts --------------------------------------------------------------

const attemptColumns = `id, endpoint_id, event_id, attempt_number, status, response_status,
	error, next_attempt_at, attempted_at, created_at`

func scanAttempt(row pgx.Row) (*Attempt, error) {
	var a Attempt
	err := row.Scan(&a.ID, &a.EndpointID, &a.EventID, &a.AttemptNumber, &a.Status,
		&a.ResponseStatus, &a.Error, &a.NextAttemptAt, &a.AttemptedAt, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// EnqueueForEvent creates a first pending attempt for every active endpoint.
// It runs on the caller's db handle so the worker can call it inside the
// transaction that marks the event succeeded.
func (s *Store) EnqueueForEvent(ctx context.Context, db database.DB, eventID uuid.UUID) (int64, error) {
	tag, err := db.Exec(ctx, `
		INSERT INTO webhook_attempts (id, endpoint_id, event_id, attempt_number)
		SELECT gen_random_uuid(), id, $1, 1
		FROM webhook_endpoints
		WHERE active
		ON CONFLICT (endpoint_id, event_id, attempt_number) DO NOTHING`, eventID)
	if err != nil {
		return 0, fmt.Errorf("enqueue webhook attempts: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ListAttemptsForEvent returns every attempt for an event, oldest first.
func (s *Store) ListAttemptsForEvent(ctx context.Context, eventID uuid.UUID) ([]Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+attemptColumns+` FROM webhook_attempts
		WHERE event_id = $1
		ORDER BY endpoint_id, attempt_number`, eventID)
	if err != nil {
		return nil, fmt.Errorf("list webhook attempts: %w", err)
	}
	defer rows.Close()

	out := []Attempt{}
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, fmt.Errorf("scan webhook attempt: %w", err)
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// Delivery is a claimed attempt together with the endpoint it targets.
type Delivery struct {
	Attempt  Attempt
	Endpoint Endpoint
}

// ErrNoneDue is returned by ClaimDue when the queue is empty.
var ErrNoneDue = fmt.Errorf("no webhook attempts due")

// ClaimDue atomically moves the oldest due pending attempt to 'delivering'
// and returns it with its endpoint. Safe under concurrent dispatchers.
func (s *Store) ClaimDue(ctx context.Context) (*Delivery, error) {
	row := s.pool.QueryRow(ctx, `
		WITH claimed AS (
			UPDATE webhook_attempts
			SET status = 'delivering', attempted_at = now()
			WHERE id = (
				SELECT id FROM webhook_attempts
				WHERE status = 'pending' AND next_attempt_at <= now()
				ORDER BY next_attempt_at, created_at
				FOR UPDATE SKIP LOCKED
				LIMIT 1
			)
			RETURNING `+attemptColumns+`
		)
		SELECT c.id, c.endpoint_id, c.event_id, c.attempt_number, c.status, c.response_status,
		       c.error, c.next_attempt_at, c.attempted_at, c.created_at,
		       e.id, e.url, e.secret, e.active, e.created_at
		FROM claimed c
		JOIN webhook_endpoints e ON e.id = c.endpoint_id`)

	var d Delivery
	a, ep := &d.Attempt, &d.Endpoint
	err := row.Scan(
		&a.ID, &a.EndpointID, &a.EventID, &a.AttemptNumber, &a.Status, &a.ResponseStatus,
		&a.Error, &a.NextAttemptAt, &a.AttemptedAt, &a.CreatedAt,
		&ep.ID, &ep.URL, &ep.secret, &ep.Active, &ep.CreatedAt,
	)
	if database.IsNotFound(err) {
		return nil, ErrNoneDue
	}
	if err != nil {
		return nil, fmt.Errorf("claim webhook attempt: %w", err)
	}
	ep.HasSecret = ep.secret != nil && *ep.secret != ""
	return &d, nil
}

// Outcome records what happened when an attempt was delivered.
type Outcome struct {
	Succeeded      bool
	ResponseStatus *int
	Error          string
	// RetryAt, if non-nil on failure, schedules a follow-up attempt.
	RetryAt *time.Time
}

// RecordOutcome finalises a 'delivering' attempt and, when the outcome calls
// for it, inserts the next pending attempt in the same transaction so a
// failure can never be recorded without its retry being queued.
func (s *Store) RecordOutcome(ctx context.Context, a Attempt, o Outcome) error {
	return database.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		status := AttemptFailed
		if o.Succeeded {
			status = AttemptSucceeded
		}
		var errText *string
		if o.Error != "" {
			errText = &o.Error
		}
		tag, err := tx.Exec(ctx, `
			UPDATE webhook_attempts
			SET status = $2, response_status = $3, error = $4, attempted_at = now()
			WHERE id = $1 AND status = 'delivering'`, a.ID, status, o.ResponseStatus, errText)
		if err != nil {
			return fmt.Errorf("record webhook outcome: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("record webhook outcome %s: attempt no longer 'delivering'", a.ID)
		}

		if !o.Succeeded && o.RetryAt != nil {
			_, err := tx.Exec(ctx, `
				INSERT INTO webhook_attempts (id, endpoint_id, event_id, attempt_number, next_attempt_at)
				VALUES ($1, $2, $3, $4, $5)`,
				uuid.New(), a.EndpointID, a.EventID, a.AttemptNumber+1, *o.RetryAt)
			if err != nil {
				return fmt.Errorf("schedule webhook retry: %w", err)
			}
		}
		return nil
	})
}

// Release returns a 'delivering' attempt to 'pending' without recording an
// outcome, for use when the dispatcher is interrupted mid-delivery.
func (s *Store) Release(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE webhook_attempts SET status = 'pending', attempted_at = NULL
		WHERE id = $1 AND status = 'delivering'`, id)
	if err != nil {
		return fmt.Errorf("release webhook attempt: %w", err)
	}
	return nil
}

// ReclaimStale returns attempts stuck in 'delivering' longer than olderThan
// to 'pending'.
func (s *Store) ReclaimStale(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE webhook_attempts SET status = 'pending', attempted_at = NULL
		WHERE status = 'delivering' AND attempted_at < now() - $1::interval`, olderThan)
	if err != nil {
		return 0, fmt.Errorf("reclaim stale webhook attempts: %w", err)
	}
	return tag.RowsAffected(), nil
}
