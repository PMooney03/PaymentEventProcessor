// Package worker processes pending events asynchronously. Any number of
// workers, in one process or many, can run against the same database: all
// coordination goes through PostgreSQL row locks, never shared memory.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/PMooney03/payment-event-processor/internal/database"
	"github.com/PMooney03/payment-event-processor/internal/events"
	"github.com/PMooney03/payment-event-processor/internal/retry"
)

// Processor performs the downstream work for a claimed event. A nil return
// marks the event succeeded; an error schedules a retry or, once retries are
// exhausted, marks it failed.
type Processor func(ctx context.Context, e *events.Event) error

// SuccessHook runs inside the transaction that marks an event succeeded.
// The webhook package uses it to enqueue deliveries atomically with the
// status change (a transactional outbox), so a success can never be recorded
// without its webhook being queued, or vice versa.
type SuccessHook func(ctx context.Context, tx database.DB, e *events.Event) error

// Config tunes a Worker.
type Config struct {
	Concurrency    int
	PollInterval   time.Duration
	MaxRetries     int
	RetryBaseDelay time.Duration
	// StaleAfter is how long an event may sit in 'processing' before it is
	// assumed orphaned by a crashed worker and returned to the queue.
	StaleAfter time.Duration
}

// finalizeTimeout bounds the database writes that record an attempt's
// outcome. They use a context detached from shutdown so a claimed event is
// never left in 'processing' because the worker was asked to stop.
const finalizeTimeout = 10 * time.Second

// Worker claims and processes events.
type Worker struct {
	cfg     Config
	pool    *database.Pool
	store   *events.Store
	process Processor
	onOK    SuccessHook
	log     *slog.Logger
}

// New builds a Worker. hook may be nil.
func New(cfg Config, pool *database.Pool, process Processor, hook SuccessHook, log *slog.Logger) *Worker {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = 5 * time.Minute
	}
	return &Worker{
		cfg:     cfg,
		pool:    pool,
		store:   events.NewStore(pool),
		process: process,
		onOK:    hook,
		log:     log.With("component", "worker"),
	}
}

// Run starts cfg.Concurrency processing loops plus a stale-event reaper and
// blocks until ctx is cancelled and every loop has exited.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.loop(ctx, w.log.With("worker_id", id))
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.reapLoop(ctx)
	}()
	wg.Wait()
	w.log.Info("worker stopped")
}

// loop drains the queue as fast as it can and sleeps only when it is empty.
func (w *Worker) loop(ctx context.Context, log *slog.Logger) {
	for {
		processed, err := w.ProcessOne(ctx, log)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Error("process event", "error", err)
		}
		if processed && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.cfg.PollInterval):
		}
	}
}

// ProcessOne claims at most one due event and runs it to an outcome. It
// returns false when the queue is empty. Exported so tests can drive the
// worker deterministically without the polling loop.
func (w *Worker) ProcessOne(ctx context.Context, log *slog.Logger) (bool, error) {
	e, err := w.store.ClaimPending(ctx)
	if errors.Is(err, events.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	log = log.With("event_id", e.ID, "payment_id", e.PaymentID, "type", e.Type, "attempt", e.RetryCount+1)
	start := time.Now()
	perr := w.safeProcess(ctx, e)
	elapsed := time.Since(start)

	// From here on, use a context that survives shutdown.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
	defer cancel()

	switch {
	case perr == nil:
		if err := w.succeed(fctx, e); err != nil {
			return true, err
		}
		log.Info("event processed", "status", events.StatusSucceeded, "duration_ms", ms(elapsed))

	case ctx.Err() != nil:
		// Interrupted by shutdown, not a real failure: hand it back untouched.
		if err := w.store.Release(fctx, e.ID); err != nil {
			return true, err
		}
		log.Warn("event released during shutdown", "duration_ms", ms(elapsed))

	case e.RetryCount < w.cfg.MaxRetries:
		delay := retry.Backoff(w.cfg.RetryBaseDelay, e.RetryCount)
		if err := w.store.ScheduleRetry(fctx, e.ID, time.Now().Add(delay), perr.Error()); err != nil {
			return true, err
		}
		log.Warn("event failed, retry scheduled",
			"error", perr.Error(), "retry_in", delay.String(),
			"retries_remaining", w.cfg.MaxRetries-e.RetryCount-1, "duration_ms", ms(elapsed))

	default:
		if err := w.store.MarkFailed(fctx, e.ID, perr.Error()); err != nil {
			return true, err
		}
		log.Error("event failed permanently",
			"error", perr.Error(), "retries", e.RetryCount, "duration_ms", ms(elapsed))
	}
	return true, nil
}

// safeProcess runs the Processor and converts a panic into an error so one
// bad event cannot take down the worker.
func (w *Worker) safeProcess(ctx context.Context, e *events.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("processor panicked: %v", r)
		}
	}()
	return w.process(ctx, e)
}

// succeed marks the event succeeded and runs the success hook atomically.
func (w *Worker) succeed(ctx context.Context, e *events.Event) error {
	return database.WithTx(ctx, w.pool, func(tx pgx.Tx) error {
		if err := w.store.MarkSucceeded(ctx, tx, e.ID); err != nil {
			return err
		}
		if w.onOK != nil {
			if err := w.onOK(ctx, tx, e); err != nil {
				return fmt.Errorf("success hook: %w", err)
			}
		}
		return nil
	})
}

// reapLoop periodically returns orphaned 'processing' rows to the queue.
func (w *Worker) reapLoop(ctx context.Context) {
	interval := w.cfg.StaleAfter / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.store.ReclaimStale(ctx, w.cfg.StaleAfter)
			if err != nil && ctx.Err() == nil {
				w.log.Error("reclaim stale events", "error", err)
			} else if n > 0 {
				w.log.Warn("reclaimed stale events", "count", n)
			}
		}
	}
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
