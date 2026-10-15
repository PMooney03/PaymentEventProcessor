package http

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/PMooney03/payment-event-processor/internal/logging"
)

const requestIDHeader = "X-Request-ID"

// middleware is a standard http.Handler decorator.
type middleware func(http.Handler) http.Handler

// chain applies middlewares so that the first in the list is outermost.
func chain(h http.Handler, mws ...middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// requestID ensures every request has an ID (honouring a client-supplied one
// if it is short and printable) and attaches a request-scoped logger to ctx.
func requestID(base *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(requestIDHeader)
			if !validRequestID(id) {
				id = newRequestID()
			}
			w.Header().Set(requestIDHeader, id)

			logger := base.With("request_id", id)
			ctx := logging.WithLogger(r.Context(), logger)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func validRequestID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if c < 0x21 || c > 0x7e { // printable ASCII, no spaces
			return false
		}
	}
	return true
}

func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is essentially unrecoverable; fall back to time.
		return hex.EncodeToString([]byte(time.Now().Format(time.RFC3339Nano)))[:16]
	}
	return hex.EncodeToString(b[:])
}

// statusRecorder captures the status code written by a handler.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// accessLog emits one structured log line per request.
func accessLog() middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}

			next.ServeHTTP(rec, r)

			if rec.status == 0 {
				rec.status = http.StatusOK
			}
			logging.FromContext(r.Context()).Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"duration_ms", float64(time.Since(start).Microseconds())/1000,
			)
		})
	}
}

// recoverPanic converts a handler panic into a 500 instead of killing the
// connection, and logs the stack trace.
func recoverPanic() middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if rec == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity per net/http docs
						panic(rec)
					}
					logging.FromContext(r.Context()).Error("panic recovered",
						"panic", rec, "stack", string(debug.Stack()))
					writeError(w, r, http.StatusInternalServerError, "internal_error", "an unexpected error occurred", nil)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
