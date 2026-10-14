// Command migrate applies, rolls back, or reports database migrations.
//
// Usage:
//
//	migrate up        apply all pending migrations
//	migrate down      roll back the most recent migration
//	migrate version   print the current schema version
//
// The database is selected with the DATABASE_URL environment variable.
package main

import (
	"fmt"
	"os"

	"github.com/PMooney03/payment-event-processor/internal/config"
	"github.com/PMooney03/payment-event-processor/internal/database"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: migrate <up|down|version>")
	}

	url := config.DatabaseURL()
	mg, err := database.NewMigrator(url)
	if err != nil {
		return err
	}
	defer mg.Close()

	switch args[0] {
	case "up":
		if err := mg.Up(); err != nil {
			return err
		}
	case "down":
		if err := mg.Down(); err != nil {
			return err
		}
	case "version":
		// handled below
	default:
		return fmt.Errorf("unknown command %q (want up, down or version)", args[0])
	}

	v, dirty, err := mg.Version()
	if err != nil {
		return err
	}
	fmt.Printf("schema version: %d (dirty=%t)\n", v, dirty)
	return nil
}
