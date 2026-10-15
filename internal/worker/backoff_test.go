package worker

import (
	"testing"
	"time"
)

func TestBackoffSequence(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	for attempt, w := range want {
		if got := Backoff(time.Second, attempt); got != w {
			t.Errorf("Backoff(1s, %d) = %s, want %s", attempt, got, w)
		}
	}
}

func TestBackoffIsCappedAndSafe(t *testing.T) {
	if got := Backoff(time.Second, 30); got != MaxBackoff {
		t.Errorf("large attempt should hit cap: %s", got)
	}
	if got := Backoff(time.Second, 100); got != MaxBackoff {
		t.Errorf("huge attempt should not overflow: %s", got)
	}
	if got := Backoff(time.Second, -3); got != time.Second {
		t.Errorf("negative attempt should behave as 0: %s", got)
	}
	if got := Backoff(100*time.Millisecond, 2); got != 400*time.Millisecond {
		t.Errorf("sub-second base: %s", got)
	}
}
