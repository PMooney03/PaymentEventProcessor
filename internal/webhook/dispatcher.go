package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/PMooney03/payment-event-processor/internal/database"
	"github.com/PMooney03/payment-event-processor/internal/events"
	"github.com/PMooney03/payment-event-processor/internal/retry"
)

// Config tunes the Dispatcher.
type Config struct {
	Concurrency    int
	PollInterval   time.Duration
	Timeout        time.Duration
	MaxRetries     int
	RetryBaseDelay time.Duration
	StaleAfter     time.Duration
}

// Payload is the JSON body POSTed to endpoints.
type Payload struct {
	ID        uuid.UUID     `json:"id"` // attempt id; receivers can use it to deduplicate
	Type      string        `json:"type"`
	Attempt   int           `json:"attempt"`
	CreatedAt time.Time     `json:"created_at"`
	Event     *events.Event `json:"event"`
}

const (
	headerEventID   = "X-Webhook-Event-ID"
	headerAttempt   = "X-Webhook-Attempt"
	headerSignature = "X-Webhook-Signature"
	userAgent       = "payment-event-processor/1.0"
	// finalizeTimeout bounds the DB write recording an outcome after shutdown.
	finalizeTimeout = 10 * time.Second
	// maxDrain limits how much of a response body is read before closing, so
	// the connection can be reused without buffering arbitrary data.
	maxDrain = 4 << 10
)

// Dispatcher delivers queued webhook attempts.
type Dispatcher struct {
	cfg    Config
	store  *Store
	events *events.Store
	client *http.Client
	log    *slog.Logger
}

// NewDispatcher builds a Dispatcher. client may be nil to use a default with
// cfg.Timeout applied.
func NewDispatcher(cfg Config, pool *database.Pool, client *http.Client, log *slog.Logger) *Dispatcher {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = 5 * time.Minute
	}
	if client == nil {
		client = &http.Client{
			Timeout: cfg.Timeout,
			// Do not follow redirects: a redirect to an unexpected host would
			// leak signed payloads.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return &Dispatcher{
		cfg:    cfg,
		store:  NewStore(pool),
		events: events.NewStore(pool),
		client: client,
		log:    log.With("component", "webhook_dispatcher"),
	}
}

// SuccessHook returns a worker hook that enqueues deliveries for a succeeded
// event inside the worker's transaction.
func SuccessHook(store *Store) func(ctx context.Context, tx database.DB, e *events.Event) error {
	return func(ctx context.Context, tx database.DB, e *events.Event) error {
		_, err := store.EnqueueForEvent(ctx, tx, e.ID)
		return err
	}
}

// Run starts the delivery loops and blocks until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < d.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			d.loop(ctx, d.log.With("dispatcher_id", id))
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.reapLoop(ctx)
	}()
	wg.Wait()
	d.log.Info("webhook dispatcher stopped")
}

func (d *Dispatcher) loop(ctx context.Context, log *slog.Logger) {
	for {
		delivered, err := d.DeliverOne(ctx, log)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Error("deliver webhook", "error", err)
		}
		if delivered && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.cfg.PollInterval):
		}
	}
}

// DeliverOne claims and delivers at most one attempt. It returns false when
// nothing is due.
func (d *Dispatcher) DeliverOne(ctx context.Context, log *slog.Logger) (bool, error) {
	dl, err := d.store.ClaimDue(ctx)
	if errors.Is(err, ErrNoneDue) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	a := dl.Attempt
	log = log.With("attempt_id", a.ID, "event_id", a.EventID, "endpoint_id", a.EndpointID, "attempt", a.AttemptNumber)

	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
	defer cancel()

	ev, err := d.events.Get(ctx, a.EventID)
	if err != nil {
		// The event vanished (or the DB is unhappy); don't lose the attempt.
		if relErr := d.store.Release(fctx, a.ID); relErr != nil {
			return true, errors.Join(err, relErr)
		}
		return true, fmt.Errorf("load event for webhook: %w", err)
	}

	start := time.Now()
	outcome := d.send(ctx, dl, ev)
	elapsed := time.Since(start)

	if ctx.Err() != nil && !outcome.Succeeded {
		// Interrupted by shutdown rather than rejected by the receiver.
		if err := d.store.Release(fctx, a.ID); err != nil {
			return true, err
		}
		log.Warn("webhook attempt released during shutdown")
		return true, nil
	}

	if !outcome.Succeeded && a.AttemptNumber <= d.cfg.MaxRetries {
		at := time.Now().Add(retry.Backoff(d.cfg.RetryBaseDelay, a.AttemptNumber-1))
		outcome.RetryAt = &at
	}
	if err := d.store.RecordOutcome(fctx, a, outcome); err != nil {
		return true, err
	}

	attrs := []any{"payment_id", ev.PaymentID, "duration_ms", float64(elapsed.Microseconds()) / 1000}
	if outcome.ResponseStatus != nil {
		attrs = append(attrs, "response_status", *outcome.ResponseStatus)
	}
	switch {
	case outcome.Succeeded:
		log.Info("webhook delivered", attrs...)
	case outcome.RetryAt != nil:
		log.Warn("webhook delivery failed, retry scheduled",
			append(attrs, "error", outcome.Error, "retry_at", outcome.RetryAt.Format(time.RFC3339))...)
	default:
		log.Error("webhook delivery failed permanently",
			append(attrs, "error", outcome.Error, "attempts", a.AttemptNumber)...)
	}
	return true, nil
}

// send performs the HTTP POST and classifies the result. Only 2xx counts as
// success; anything else, including transport errors, is a failure.
func (d *Dispatcher) send(ctx context.Context, dl *Delivery, ev *events.Event) Outcome {
	body, err := json.Marshal(Payload{
		ID:        dl.Attempt.ID,
		Type:      ev.Type,
		Attempt:   dl.Attempt.AttemptNumber,
		CreatedAt: time.Now().UTC(),
		Event:     ev,
	})
	if err != nil {
		return Outcome{Error: "encode payload: " + err.Error()}
	}

	reqCtx, cancel := context.WithTimeout(ctx, d.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, dl.Endpoint.URL, bytes.NewReader(body))
	if err != nil {
		return Outcome{Error: "build request: " + err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set(headerEventID, ev.ID.String())
	req.Header.Set(headerAttempt, strconv.Itoa(dl.Attempt.AttemptNumber))
	if dl.Endpoint.secret != nil && *dl.Endpoint.secret != "" {
		req.Header.Set(headerSignature, Sign(*dl.Endpoint.secret, body))
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return Outcome{Error: sanitizeTransportError(err)}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))

	status := resp.StatusCode
	if status >= 200 && status < 300 {
		return Outcome{Succeeded: true, ResponseStatus: &status}
	}
	return Outcome{ResponseStatus: &status, Error: "endpoint returned HTTP " + strconv.Itoa(status)}
}

// Sign computes the hex HMAC-SHA256 of body under secret, in the
// "sha256=<hex>" form receivers commonly expect.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// sanitizeTransportError keeps error text short and generic enough to store
// and return to API clients.
func sanitizeTransportError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "request timed out"
	case errors.Is(err, context.Canceled):
		return "request cancelled"
	}
	msg := err.Error()
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return msg
}

func (d *Dispatcher) reapLoop(ctx context.Context) {
	interval := d.cfg.StaleAfter / 2
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
			n, err := d.store.ReclaimStale(ctx, d.cfg.StaleAfter)
			if err != nil && ctx.Err() == nil {
				d.log.Error("reclaim stale webhook attempts", "error", err)
			} else if n > 0 {
				d.log.Warn("reclaimed stale webhook attempts", "count", n)
			}
		}
	}
}
