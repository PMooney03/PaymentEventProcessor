package events

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/PMooney03/payment-event-processor/internal/database"
)

// Store persists events in PostgreSQL. It is safe for concurrent use.
type Store struct {
	pool *database.Pool
}

// NewStore returns a Store backed by pool.
func NewStore(pool *database.Pool) *Store {
	return &Store{pool: pool}
}

const eventColumns = `id, type, payment_id, amount, currency, customer_id, status,
	retry_count, next_attempt_at, last_error, created_at, updated_at`

func scanEvent(row pgx.Row) (*Event, error) {
	var e Event
	err := row.Scan(
		&e.ID, &e.Type, &e.PaymentID, &e.Amount, &e.Currency, &e.CustomerID, &e.Status,
		&e.RetryCount, &e.NextAttemptAt, &e.LastError, &e.CreatedAt, &e.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	e.Currency = strings.TrimSpace(e.Currency) // CHAR(3) pads; keep JSON clean
	return &e, nil
}

// Insert writes a new event using db, which may be the pool or an open
// transaction. The event's ID and timestamps are filled in from the row
// PostgreSQL returns, so the caller sees exactly what was stored.
func (s *Store) Insert(ctx context.Context, db database.DB, in Input) (*Event, error) {
	row := db.QueryRow(ctx, `
		INSERT INTO events (id, type, payment_id, amount, currency, customer_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING `+eventColumns,
		uuid.New(), in.Type, in.PaymentID, in.Amount, in.Currency, in.CustomerID,
	)
	e, err := scanEvent(row)
	if err != nil {
		return nil, fmt.Errorf("insert event: %w", err)
	}
	return e, nil
}

// Get returns the event with the given ID or ErrNotFound.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (*Event, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+eventColumns+` FROM events WHERE id = $1`, id)
	e, err := scanEvent(row)
	if database.IsNotFound(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get event: %w", err)
	}
	return e, nil
}

// List returns events newest-first, applying the optional filters, using
// keyset pagination on (created_at, id).
func (s *Store) List(ctx context.Context, f ListFilter) (*Page, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	var (
		where []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if f.Status != "" {
		where = append(where, "status = "+arg(f.Status))
	}
	if f.Type != "" {
		where = append(where, "type = "+arg(f.Type))
	}
	if f.Cursor != uuid.Nil {
		// Continue strictly after the cursor row in descending order.
		where = append(where, fmt.Sprintf(
			"(created_at, id) < (SELECT created_at, id FROM events WHERE id = %s)", arg(f.Cursor)))
	}

	query := `SELECT ` + eventColumns + ` FROM events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	// Fetch one extra row to learn whether another page exists.
	query += " ORDER BY created_at DESC, id DESC LIMIT " + arg(limit+1)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	page := &Page{Events: make([]Event, 0, limit)}
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		page.Events = append(page.Events, *e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}

	if len(page.Events) > limit {
		page.Events = page.Events[:limit]
		last := page.Events[limit-1].ID
		page.NextCursor = &last
	}
	return page, nil
}

// ClaimPending atomically moves the oldest due pending event to 'processing'
// and returns it. FOR UPDATE SKIP LOCKED makes this safe under any number of
// concurrent workers: each row is handed to exactly one claimant, and workers
// never block each other. Returns ErrNotFound when nothing is due.
func (s *Store) ClaimPending(ctx context.Context) (*Event, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE events
		SET status = 'processing', updated_at = now()
		WHERE id = (
			SELECT id FROM events
			WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING `+eventColumns)
	e, err := scanEvent(row)
	if database.IsNotFound(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("claim pending event: %w", err)
	}
	return e, nil
}

// MarkSucceeded finalises a processing event as succeeded.
func (s *Store) MarkSucceeded(ctx context.Context, db database.DB, id uuid.UUID) error {
	return s.transition(ctx, db, id, `
		UPDATE events
		SET status = 'succeeded', last_error = NULL, updated_at = now()
		WHERE id = $1 AND status = 'processing'`)
}

// ScheduleRetry returns a processing event to the queue with an incremented
// retry counter and a future next_attempt_at.
func (s *Store) ScheduleRetry(ctx context.Context, id uuid.UUID, nextAttemptAt time.Time, reason string) error {
	return s.transition(ctx, s.pool, id, `
		UPDATE events
		SET status = 'pending', retry_count = retry_count + 1,
		    next_attempt_at = $2, last_error = $3, updated_at = now()
		WHERE id = $1 AND status = 'processing'`, nextAttemptAt, reason)
}

// Release returns a processing event to the queue immediately without
// counting a retry. Used when a worker is shutting down mid-attempt.
func (s *Store) Release(ctx context.Context, id uuid.UUID) error {
	return s.transition(ctx, s.pool, id, `
		UPDATE events
		SET status = 'pending', next_attempt_at = now(), updated_at = now()
		WHERE id = $1 AND status = 'processing'`)
}

// ReclaimStale returns events that have been 'processing' for longer than
// olderThan back to 'pending'. This recovers rows orphaned by a worker that
// crashed between claiming and finishing. It returns how many were reclaimed.
func (s *Store) ReclaimStale(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE events
		SET status = 'pending', next_attempt_at = now(), updated_at = now(),
		    last_error = 'reclaimed: worker did not finish processing'
		WHERE status = 'processing' AND updated_at < now() - $1::interval`,
		olderThan)
	if err != nil {
		return 0, fmt.Errorf("reclaim stale events: %w", err)
	}
	return tag.RowsAffected(), nil
}

// MarkFailed finalises a processing event as permanently failed.
func (s *Store) MarkFailed(ctx context.Context, id uuid.UUID, reason string) error {
	return s.transition(ctx, s.pool, id, `
		UPDATE events
		SET status = 'failed', last_error = $2, updated_at = now()
		WHERE id = $1 AND status = 'processing'`, reason)
}

// transition runs a status-changing UPDATE that is guarded by the current
// status. Affecting zero rows means another actor changed the row first,
// which indicates a bug in the claim protocol, so it is reported loudly.
func (s *Store) transition(ctx context.Context, db database.DB, id uuid.UUID, query string, extra ...any) error {
	args := append([]any{id}, extra...)
	tag, err := db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update event %s: %w", id, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("update event %s: expected 1 row in 'processing', affected %d", id, tag.RowsAffected())
	}
	return nil
}
