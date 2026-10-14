package config

import (
	"strings"
	"testing"
	"time"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(envFrom(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want 8080", cfg.Port)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", cfg.MaxRetries)
	}
	if cfg.WorkerInterval != time.Second {
		t.Errorf("WorkerInterval = %s, want 1s", cfg.WorkerInterval)
	}
	if cfg.WebhookTimeout != 5*time.Second {
		t.Errorf("WebhookTimeout = %s, want 5s", cfg.WebhookTimeout)
	}
	if cfg.Addr() != ":8080" {
		t.Errorf("Addr() = %q, want :8080", cfg.Addr())
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(envFrom(map[string]string{
		"DATABASE_URL":       "postgres://u:p@db:5432/x",
		"PORT":               "9000",
		"LOG_LEVEL":          "debug",
		"MAX_RETRIES":        "7",
		"RETRY_BASE_DELAY":   "250ms",
		"WORKER_INTERVAL":    "2s",
		"WORKER_CONCURRENCY": "8",
		"FAILURE_RATE":       "0",
		"WEBHOOK_TIMEOUT":    "10s",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DatabaseURL != "postgres://u:p@db:5432/x" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.Port != 9000 || cfg.MaxRetries != 7 || cfg.WorkerConcurrency != 8 {
		t.Errorf("ints not applied: %+v", cfg)
	}
	if cfg.RetryBaseDelay != 250*time.Millisecond || cfg.WorkerInterval != 2*time.Second || cfg.WebhookTimeout != 10*time.Second {
		t.Errorf("durations not applied: %+v", cfg)
	}
	if cfg.FailureRate != 0 {
		t.Errorf("FailureRate = %v, want 0", cfg.FailureRate)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := load(envFrom(map[string]string{
		"PORT":            "notanumber",
		"MAX_RETRIES":     "-1",
		"WORKER_INTERVAL": "soon",
		"FAILURE_RATE":    "1.5",
		"LOG_LEVEL":       "loud",
	}))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	for _, want := range []string{"PORT", "MAX_RETRIES", "WORKER_INTERVAL", "FAILURE_RATE", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %s: %v", want, err)
		}
	}
}
