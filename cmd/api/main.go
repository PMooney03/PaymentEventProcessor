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
	"sync"
	"syscall"
	"time"

	"github.com/PMooney03/payment-event-processor/internal/config"
	"github.com/PMooney03/payment-event-processor/internal/database"
	"github.com/PMooney03/payment-event-processor/internal/events"
	apihttp "github.com/PMooney03/payment-event-processor/internal/http"
	"github.com/PMooney03/payment-event-processor/internal/logging"
	"github.com/PMooney03/payment-event-processor/internal/webhook"
	"github.com/PMooney03/payment-event-processor/internal/worker"
)

const (
	shutdownTimeout = 15 * time.Second
	// staleAfter is how long a claimed event or webhook attempt may sit
	// in-flight before a crashed worker is assumed and it is re-queued.
	staleAfter = 5 * time.Minute
)

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
	webhookStore := webhook.NewStore(pool)

	// Background worker: simulated downstream with configurable flakiness.
	// On success it enqueues webhook deliveries in the same transaction.
	wrk := worker.New(worker.Config{
		Concurrency:    cfg.WorkerConcurrency,
		PollInterval:   cfg.WorkerInterval,
		MaxRetries:     cfg.MaxRetries,
		RetryBaseDelay: cfg.RetryBaseDelay,
		StaleAfter:     staleAfter,
	}, pool,
		worker.Simulated(cfg.FailureRate, 50*time.Millisecond, 250*time.Millisecond),
		webhook.SuccessHook(webhookStore),
		logger)

	dispatcher := webhook.NewDispatcher(webhook.Config{
		Concurrency:    cfg.WorkerConcurrency,
		PollInterval:   cfg.WorkerInterval,
		Timeout:        cfg.WebhookTimeout,
		MaxRetries:     cfg.WebhookMaxRetries,
		RetryBaseDelay: cfg.RetryBaseDelay,
		StaleAfter:     staleAfter,
	}, pool, nil, logger)

	var background sync.WaitGroup
	background.Add(2)
	go func() {
		defer background.Done()
		logger.Info("worker started", "concurrency", cfg.WorkerConcurrency, "max_retries", cfg.MaxRetries)
		wrk.Run(ctx)
	}()
	go func() {
		defer background.Done()
		logger.Info("webhook dispatcher started", "timeout", cfg.WebhookTimeout.String(), "max_retries", cfg.WebhookMaxRetries)
		dispatcher.Run(ctx)
	}()

	srv := &http.Server{
		Addr: cfg.Addr(),
		Handler: apihttp.NewHandler(apihttp.Deps{
			Logger:   logger,
			Events:   eventSvc,
			Webhooks: webhookStore,
			DB:       pool,
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

	// The worker observes ctx cancellation itself; wait for it to release or
	// finish any in-flight event, but not forever.
	stop() // ensure ctx is cancelled even if we got here via serverErr
	workersDone := make(chan struct{})
	go func() { background.Wait(); close(workersDone) }()
	select {
	case <-workersDone:
		logger.Info("background workers stopped")
	case <-shutdownCtx.Done():
		logger.Warn("background workers did not stop in time")
	}
	return nil
}
