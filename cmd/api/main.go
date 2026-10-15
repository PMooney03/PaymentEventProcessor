// Command api runs the Payment Event Processor HTTP server, background
// worker, and webhook dispatcher.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/PMooney03/payment-event-processor/internal/config"
	"github.com/PMooney03/payment-event-processor/internal/database"
	"github.com/PMooney03/payment-event-processor/internal/events"
	apihttp "github.com/PMooney03/payment-event-processor/internal/http"
	"github.com/PMooney03/payment-event-processor/internal/logging"
)

const shutdownTimeout = 15 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := logging.NewDefault(cfg.LogLevel)

	// Root context is cancelled on SIGINT/SIGTERM; everything derives from it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	logger.Info("database connected")

	eventSvc := events.NewService(pool)

	srv := &http.Server{
		Addr: cfg.Addr(),
		Handler: apihttp.NewHandler(apihttp.Deps{
			Logger: logger,
			Events: eventSvc,
			DB:     pool,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.Addr())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("http server: %w", err)
		}
		close(serverErr)
	}()

	// Block until a signal arrives or the server fails to start.
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			return err
		}
	}

	// Stop accepting new requests and let in-flight ones finish.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	logger.Info("http server stopped")
	return nil
}
