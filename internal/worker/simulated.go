package worker

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/PMooney03/payment-event-processor/internal/events"
)

// ErrSimulatedFailure is returned by the simulated downstream when it
// "fails". It stands in for a timeout or 5xx from a real payment provider.
var ErrSimulatedFailure = errors.New("simulated downstream failure")

// ForceFailMarker in a payment_id makes the simulated downstream fail every
// time, which is handy for demonstrating the retry and dead-letter path
// without relying on randomness.
const ForceFailMarker = "fail"

// Simulated returns a Processor that imitates a flaky downstream dependency:
// it sleeps for a random latency between minLatency and maxLatency and then
// fails with probability failureRate.
func Simulated(failureRate float64, minLatency, maxLatency time.Duration) Processor {
	if maxLatency < minLatency {
		maxLatency = minLatency
	}
	return func(ctx context.Context, e *events.Event) error {
		latency := minLatency
		if span := maxLatency - minLatency; span > 0 {
			latency += time.Duration(rand.Int64N(int64(span)))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(latency):
		}

		if strings.Contains(strings.ToLower(e.PaymentID), ForceFailMarker) {
			return ErrSimulatedFailure
		}
		if failureRate > 0 && rand.Float64() < failureRate {
			return ErrSimulatedFailure
		}
		return nil
	}
}
