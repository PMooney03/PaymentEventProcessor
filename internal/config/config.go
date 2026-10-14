// Package config loads runtime configuration from environment variables with
// sensible defaults for local development.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/payments?sslmode=disable"

// Config is the fully resolved application configuration.
type Config struct {
	DatabaseURL string
	Port        int
	LogLevel    string

	// Event processing
	MaxRetries        int
	RetryBaseDelay    time.Duration
	WorkerInterval    time.Duration
	WorkerConcurrency int
	FailureRate       float64

	// Webhook delivery
	WebhookTimeout    time.Duration
	WebhookMaxRetries int
}

// Load reads configuration from the process environment. It returns an error
// describing every invalid value rather than stopping at the first one.
func Load() (Config, error) {
	return load(os.Getenv)
}

// DatabaseURL returns only the database URL. It exists for the migrate
// command, which needs nothing else.
func DatabaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return defaultDatabaseURL
}

// load is the testable core of Load; getenv abstracts os.Getenv.
func load(getenv func(string) string) (Config, error) {
	p := parser{getenv: getenv}

	cfg := Config{
		DatabaseURL:       p.str("DATABASE_URL", defaultDatabaseURL),
		Port:              p.intRange("PORT", 8080, 1, 65535),
		LogLevel:          p.str("LOG_LEVEL", "info"),
		MaxRetries:        p.intRange("MAX_RETRIES", 3, 0, 100),
		RetryBaseDelay:    p.duration("RETRY_BASE_DELAY", time.Second),
		WorkerInterval:    p.duration("WORKER_INTERVAL", time.Second),
		WorkerConcurrency: p.intRange("WORKER_CONCURRENCY", 2, 1, 64),
		FailureRate:       p.floatRange("FAILURE_RATE", 0.3, 0, 1),
		WebhookTimeout:    p.duration("WEBHOOK_TIMEOUT", 5*time.Second),
		WebhookMaxRetries: p.intRange("WEBHOOK_MAX_RETRIES", 5, 0, 100),
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		p.errs = append(p.errs, fmt.Errorf("LOG_LEVEL: %q is not one of debug, info, warn, error", cfg.LogLevel))
	}

	if len(p.errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", errors.Join(p.errs...))
	}
	return cfg, nil
}

// Addr returns the listen address for the HTTP server.
func (c Config) Addr() string {
	return fmt.Sprintf(":%d", c.Port)
}

// parser accumulates parse errors so Load can report them all at once.
type parser struct {
	getenv func(string) string
	errs   []error
}

func (p *parser) str(key, def string) string {
	if v := p.getenv(key); v != "" {
		return v
	}
	return def
}

func (p *parser) intRange(key string, def, min, max int) int {
	raw := p.getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not an integer", key, raw))
		return def
	}
	if v < min || v > max {
		p.errs = append(p.errs, fmt.Errorf("%s: %d is outside [%d, %d]", key, v, min, max))
		return def
	}
	return v
}

func (p *parser) floatRange(key string, def, min, max float64) float64 {
	raw := p.getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a number", key, raw))
		return def
	}
	if v < min || v > max {
		p.errs = append(p.errs, fmt.Errorf("%s: %g is outside [%g, %g]", key, v, min, max))
		return def
	}
	return v
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	raw := p.getenv(key)
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a duration (e.g. 500ms, 2s)", key, raw))
		return def
	}
	if v <= 0 {
		p.errs = append(p.errs, fmt.Errorf("%s: must be positive, got %s", key, v))
		return def
	}
	return v
}
