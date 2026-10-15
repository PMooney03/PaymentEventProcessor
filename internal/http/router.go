package http

import (
	"log/slog"
	"net/http"
)

// Deps are the collaborators the router needs.
type Deps struct {
	Logger   *slog.Logger
	Events   EventService
	Webhooks WebhookService
	DB       Pinger
}

// NewHandler builds the fully wired HTTP handler.
func NewHandler(d Deps) http.Handler {
	mux := http.NewServeMux()

	hh := &healthHandler{db: d.DB}
	mux.HandleFunc("GET /health", hh.health)
	mux.HandleFunc("GET /ready", hh.ready)

	eh := &eventsHandler{svc: d.Events}
	mux.HandleFunc("POST /api/events", eh.create)
	mux.HandleFunc("GET /api/events", eh.list)
	mux.HandleFunc("GET /api/events/{id}", eh.get)

	wh := &webhooksHandler{svc: d.Webhooks}
	mux.HandleFunc("POST /api/webhooks", wh.create)
	mux.HandleFunc("GET /api/webhooks", wh.list)
	mux.HandleFunc("DELETE /api/webhooks/{id}", wh.remove)
	mux.HandleFunc("GET /api/events/{id}/webhook-attempts", wh.attemptsForEvent)

	return chain(jsonFallback(mux),
		requestID(d.Logger),
		recoverPanic(),
		accessLog(),
	)
}

// jsonFallback makes ServeMux's built-in 404 and 405 responses use the same
// JSON error envelope as the rest of the API. Registering a catch-all "/"
// pattern would be simpler but suppresses ServeMux's 405 detection.
func jsonFallback(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		// No pattern matched: let the mux decide between 404 and 405 (and set
		// the Allow header) but swallow its plain-text body.
		probe := &headerProbe{header: http.Header{}}
		mux.ServeHTTP(probe, r)

		if allow := probe.header.Get("Allow"); allow != "" {
			w.Header().Set("Allow", allow)
		}
		switch probe.status {
		case http.StatusMethodNotAllowed:
			writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed for this route", nil)
		default:
			writeError(w, r, http.StatusNotFound, "not_found", "no such route", nil)
		}
	})
}

// headerProbe is a ResponseWriter that records the status and headers and
// discards the body.
type headerProbe struct {
	header http.Header
	status int
}

func (p *headerProbe) Header() http.Header         { return p.header }
func (p *headerProbe) WriteHeader(code int)        { p.status = code }
func (p *headerProbe) Write(b []byte) (int, error) { return len(b), nil }
