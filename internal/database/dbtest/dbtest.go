// Package dbtest provides helpers for integration tests that need a real
// PostgreSQL database. Tests are skipped unless TEST_DATABASE_URL is set, so
// the default `go test ./...` stays fast and dependency-free.
package dbtest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/PMooney03/payment-event-processor/internal/database"
)

// EnvVar names the environment variable holding the test database URL.
const EnvVar = "TEST_DATABASE_URL"

// Pool connects to the test database, applies all migrations, and truncates
// every application table so each test starts from a known-empty state.
// The pool is closed automatically when the test finishes.
func Pool(t testing.TB) *database.Pool {
	t.Helper()

	url := os.Getenv(EnvVar)
	if url == "" {
		t.Skipf("%s not set; skipping database integration test", EnvVar)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mg, err := database.NewMigrator(url)
	if err != nil {
		t.Fatalf("dbtest: new migrator: %v", err)
	}
	if err := mg.Up(); err != nil {
		t.Fatalf("dbtest: migrate up: %v", err)
	}
	if err := mg.Close(); err != nil {
		t.Fatalf("dbtest: close migrator: %v", err)
	}

	pool, err := database.Connect(ctx, url)
	if err != nil {
		t.Fatalf("dbtest: connect: %v", err)
	}
	t.Cleanup(pool.Close)

	Truncate(t, pool)
	return pool
}

// Truncate empties all application tables.
func Truncate(t testing.TB, pool *database.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`TRUNCATE webhook_attempts, webhook_endpoints, idempotency_keys, events CASCADE`)
	if err != nil {
		t.Fatalf("dbtest: truncate: %v", err)
	}
}
