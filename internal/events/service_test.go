package events

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/database/dbtest"
	"github.com/PMooney03/payment-event-processor/internal/idempotency"
)

func countEvents(t *testing.T, svc *Service) int {
	t.Helper()
	var n int
	if err := svc.pool.QueryRow(context.Background(), `SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestServiceCreateWithoutKeyAlwaysInserts(t *testing.T) {
	svc := NewService(dbtest.Pool(t))
	ctx := context.Background()

	a, err := svc.Create(ctx, validInput(), "")
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.Create(ctx, validInput(), "")
	if err != nil {
		t.Fatal(err)
	}
	if a.Event.ID == b.Event.ID || a.Replayed || b.Replayed {
		t.Errorf("without a key each call must create a new event: %v %v", a, b)
	}
	if n := countEvents(t, svc); n != 2 {
		t.Errorf("events = %d, want 2", n)
	}
}

func TestServiceIdempotentReplay(t *testing.T) {
	svc := NewService(dbtest.Pool(t))
	ctx := context.Background()

	first, err := svc.Create(ctx, validInput(), "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Replayed {
		t.Error("first call must not be a replay")
	}

	second, err := svc.Create(ctx, validInput(), "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed || second.Event.ID != first.Event.ID {
		t.Errorf("second call should replay the first event: %+v", second)
	}
	if n := countEvents(t, svc); n != 1 {
		t.Errorf("events = %d, want 1", n)
	}

	// Same key, semantically identical payload with different casing/whitespace
	// is still a replay because the hash is over the normalised input.
	in := validInput()
	in.Currency = " eur"
	third, err := svc.Create(ctx, in, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if !third.Replayed {
		t.Error("normalised payload should replay")
	}

	// Different key creates a new event.
	other, err := svc.Create(ctx, validInput(), "key-2")
	if err != nil {
		t.Fatal(err)
	}
	if other.Replayed || other.Event.ID == first.Event.ID {
		t.Error("different key must create a new event")
	}
}

func TestServiceIdempotentPayloadMismatch(t *testing.T) {
	svc := NewService(dbtest.Pool(t))
	ctx := context.Background()

	if _, err := svc.Create(ctx, validInput(), "key-1"); err != nil {
		t.Fatal(err)
	}
	in := validInput()
	in.Amount = 1
	_, err := svc.Create(ctx, in, "key-1")
	if !errors.Is(err, idempotency.ErrPayloadMismatch) {
		t.Fatalf("want ErrPayloadMismatch, got %v", err)
	}
	if n := countEvents(t, svc); n != 1 {
		t.Errorf("events = %d, want 1", n)
	}
}

func TestServiceValidationFailsBeforeReservingKey(t *testing.T) {
	svc := NewService(dbtest.Pool(t))
	ctx := context.Background()

	bad := validInput()
	bad.Amount = 0
	var verr *ValidationError
	if _, err := svc.Create(ctx, bad, "key-1"); !errors.As(err, &verr) {
		t.Fatalf("want ValidationError, got %v", err)
	}

	// The key must not be burned by an invalid request.
	res, err := svc.Create(ctx, validInput(), "key-1")
	if err != nil || res.Replayed {
		t.Errorf("key should still be usable after a rejected request: %v %v", res, err)
	}
}

// TestServiceConcurrentDuplicatesCreateOneEvent is the core idempotency
// guarantee: many simultaneous requests sharing a key must result in exactly
// one event, with every caller receiving that same event.
func TestServiceConcurrentDuplicatesCreateOneEvent(t *testing.T) {
	svc := NewService(dbtest.Pool(t))
	ctx := context.Background()

	const n = 50
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[uuid.UUID]int{}
		created int
		errs    []error
	)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // maximise overlap
			res, err := svc.Create(ctx, validInput(), "shared-key")
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			ids[res.Event.ID]++
			if !res.Replayed {
				created++
			}
		}()
	}
	close(start)
	wg.Wait()

	// ErrInProgress is an acceptable transient outcome for a caller that
	// observed the reservation before the winner committed; anything else is
	// a bug.
	for _, err := range errs {
		if !errors.Is(err, idempotency.ErrInProgress) {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if len(ids) != 1 {
		t.Fatalf("callers observed %d distinct events, want 1: %v", len(ids), ids)
	}
	if created != 1 {
		t.Errorf("%d callers believe they created the event, want exactly 1", created)
	}
	if got := countEvents(t, svc); got != 1 {
		t.Errorf("events in database = %d, want 1", got)
	}
}
