package database

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // registers the postgres driver
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/PMooney03/payment-event-processor/migrations"
)

// Migrator wraps golang-migrate with the embedded migration files.
type Migrator struct {
	m *migrate.Migrate
}

// NewMigrator builds a migrator for the given database URL. golang-migrate
// expects a "postgres://" scheme; pgx-style "postgresql://" is accepted too.
func NewMigrator(databaseURL string) (*Migrator, error) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("load embedded migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("init migrator: %w", err)
	}
	return &Migrator{m: m}, nil
}

// Up applies all pending migrations. Reaching the latest version with
// nothing to do is not an error.
func (mg *Migrator) Up() error {
	if err := mg.m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// Down rolls back a single migration.
func (mg *Migrator) Down() error {
	if err := mg.m.Steps(-1); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate down: %w", err)
	}
	return nil
}

// Drop removes everything in the database. Used by integration tests to
// start from a clean schema.
func (mg *Migrator) Drop() error {
	if err := mg.m.Drop(); err != nil {
		return fmt.Errorf("migrate drop: %w", err)
	}
	return nil
}

// Version returns the current schema version and whether it is dirty.
func (mg *Migrator) Version() (uint, bool, error) {
	v, dirty, err := mg.m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("migrate version: %w", err)
	}
	return v, dirty, nil
}

// Close releases the source and database handles.
func (mg *Migrator) Close() error {
	srcErr, dbErr := mg.m.Close()
	return errors.Join(srcErr, dbErr)
}
