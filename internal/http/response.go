// Package http contains the REST API: router, handlers and middleware. It
// translates between HTTP and the domain services and holds no business logic.
package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/PMooney03/payment-event-processor/internal/logging"
)

// maxBodyBytes bounds request bodies so a client cannot exhaust memory.
const maxBodyBytes = 64 << 10 // 64 KiB

// ErrorBody is the JSON envelope for every non-2xx response.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes a single API error.
type ErrorDetail struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"`
}

// writeJSON encodes v as the response body with the given status.
func writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Headers are already sent; the best we can do is log it.
		logging.FromContext(r.Context()).Error("write response", "error", err)
	}
}

// writeError writes a structured error response.
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, fields map[string]string) {
	writeJSON(w, r, status, ErrorBody{Error: ErrorDetail{Code: code, Message: message, Fields: fields}})
}

// writeInternalError logs err with full detail and returns a generic 500 so
// internal details never leak to clients.
func writeInternalError(w http.ResponseWriter, r *http.Request, err error) {
	logging.FromContext(r.Context()).Error("internal error", "error", err)
	writeError(w, r, http.StatusInternalServerError, "internal_error", "an unexpected error occurred", nil)
}

// decodeJSON strictly decodes the request body into dst. Unknown fields,
// trailing data and oversized bodies are rejected.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxErr):
			return errors.New("request body too large")
		case errors.Is(err, io.EOF):
			return errors.New("request body is empty")
		default:
			return errors.New("request body is not valid JSON: " + err.Error())
		}
	}
	if dec.More() {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}
