package http

import (
	"context"
	"net/http"
	"time"
)

// Pinger is anything that can verify a dependency is reachable; the database
// pool satisfies it.
type Pinger interface {
	Ping(ctx context.Context) error
}

type healthHandler struct {
	db Pinger
}

// health handles GET /health: the process is up and serving.
func (h *healthHandler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

// ready handles GET /ready: the service can do useful work, which requires
// a reachable database.
func (h *healthHandler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.db.Ping(ctx); err != nil {
		writeJSON(w, r, http.StatusServiceUnavailable, map[string]any{
			"status": "unavailable",
			"checks": map[string]string{"database": "failing"},
		})
		return
	}
	writeJSON(w, r, http.StatusOK, map[string]any{
		"status": "ready",
		"checks": map[string]string{"database": "ok"},
	})
}
