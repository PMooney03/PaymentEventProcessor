package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/database/dbtest"
)

func TestStoreInsertAndGet(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()

	created, err := store.Insert(ctx, pool, validInput())
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if created.ID == uuid.Nil || created.Status != StatusPending || created.RetryCount != 0 {
		t.Errorf("unexpected defaults: %+v", created)
	}
	if created.Currency != "EUR" {
		t.Errorf("currency = %q, want EUR (no CHAR padding)", created.Currency)
	}

	got, err := store.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != created.ID || got.PaymentID != "pay_123" || got.Amount != 4999 {
		t.Errorf("get mismatch: %+v", got)
	}

	if _, err := store.Get(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStoreListFiltersAndPaginates(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()

	var ids []uuid.UUID
	for i := 0; i < 5; i++ {
		in := validInput()
		if i%2 == 0 {
			in.Type = "payment.refunded"
		}
		e, err := store.Insert(ctx, pool, in)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		ids = append(ids, e.ID)
		time.Sleep(2 * time.Millisecond) // distinct created_at for deterministic ordering
	}

	// Filter by type.
	page, err := store.List(ctx, ListFilter{Type: "payment.refunded"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Events) != 3 {
		t.Errorf("type filter: got %d, want 3", len(page.Events))
	}

	// Filter by status: nothing is processing yet.
	page, err = store.List(ctx, ListFilter{Status: StatusProcessing})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Events) != 0 {
		t.Errorf("status filter: got %d, want 0", len(page.Events))
	}

	// Paginate newest-first, 2 per page.
	var seen []uuid.UUID
	var cursor uuid.UUID
	for pages := 0; pages < 10; pages++ {
		page, err = store.List(ctx, ListFilter{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("list page: %v", err)
		}
		for _, e := range page.Events {
			seen = append(seen, e.ID)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if len(seen) != 5 {
		t.Fatalf("pagination returned %d events, want 5", len(seen))
	}
	for i := range seen {
		if seen[i] != ids[len(ids)-1-i] {
			t.Errorf("order mismatch at %d: got %s want %s", i, seen[i], ids[len(ids)-1-i])
		}
	}
}

func TestStoreClaimAndTransitions(t *testing.T) {
	pool := dbtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()

	if _, err := store.ClaimPending(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty claim: want ErrNotFound, got %v", err)
	}

	e, err := store.Insert(ctx, pool, validInput())
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	claimed, err := store.ClaimPending(ctx)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != e.ID || claimed.Status != StatusProcessing {
		t.Fatalf("claimed = %+v", claimed)
	}

	// A second claim finds nothing: the row is now 'processing'.
	if _, err := store.ClaimPending(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second claim: want ErrNotFound, got %v", err)
	}

	// Retry puts it back in the queue, but only once next_attempt_at passes.
	if err := store.ScheduleRetry(ctx, e.ID, time.Now().Add(time.Hour), "boom"); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	if _, err := store.ClaimPending(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("claim before due: want ErrNotFound, got %v", err)
	}
	got, _ := store.Get(ctx, e.ID)
	if got.Status != StatusPending || got.RetryCount != 1 || got.LastError == nil || *got.LastError != "boom" {
		t.Errorf("after retry: %+v", got)
	}

	// Make it due, claim again, and fail it permanently.
	if _, err := pool.Exec(ctx, `UPDATE events SET next_attempt_at = now() - interval '1s' WHERE id = $1`, e.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimPending(ctx); err != nil {
		t.Fatalf("claim when due: %v", err)
	}
	if err := store.MarkFailed(ctx, e.ID, "gave up"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	got, _ = store.Get(ctx, e.ID)
	if got.Status != StatusFailed {
		t.Errorf("status = %s, want failed", got.Status)
	}

	// Transitions are guarded by status: a failed row cannot be "succeeded".
	if err := store.MarkSucceeded(ctx, pool, e.ID); err == nil {
		t.Error("MarkSucceeded on a failed event should error")
	}
}
