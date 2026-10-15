package worker

import "time"

// MaxBackoff caps the exponential delay so a long-lived retry chain never
// schedules something absurdly far in the future.
const MaxBackoff = time.Hour

// Backoff returns the delay before retry number (attempt+1), using plain
// exponential growth: base * 2^attempt. With a 1s base the sequence is
// 1s, 2s, 4s, 8s, ... capped at MaxBackoff.
func Backoff(base time.Duration, attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	// 2^attempt overflows a duration long before attempt reaches 63; bail
	// out early and return the cap.
	if attempt >= 40 {
		return MaxBackoff
	}
	d := base << uint(attempt)
	if d > MaxBackoff || d <= 0 {
		return MaxBackoff
	}
	return d
}
