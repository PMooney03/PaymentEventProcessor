// Package logging configures structured JSON logging with log/slog and
// provides helpers for carrying a request-scoped logger through a context.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// New returns a JSON logger writing to w at the given level
// ("debug", "info", "warn" or "error"). Unknown levels fall back to info.
func New(w io.Writer, level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler)
}

// NewDefault returns a JSON logger writing to stdout.
func NewDefault(level string) *slog.Logger {
	return New(os.Stdout, level)
}

// Discard returns a logger that drops everything. Useful in tests.
func Discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type ctxKey struct{}

// WithLogger returns a child context carrying logger. HTTP middleware uses it
// to attach the request ID so downstream code logs with the same correlation
// fields without threading the logger through every call.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, logger)
}

// FromContext returns the logger stored in ctx, or slog.Default() if none.
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}
