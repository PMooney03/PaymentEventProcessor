package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestNewEmitsJSONWithLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "warn")

	log.Info("hidden")
	log.Warn("shown", "event_id", "evt_1")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not a single JSON object: %v\n%s", err, buf.String())
	}
	if rec["msg"] != "shown" || rec["event_id"] != "evt_1" || rec["level"] != "WARN" {
		t.Errorf("unexpected record: %v", rec)
	}
}

func TestContextRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "info").With("request_id", "req_1")

	ctx := WithLogger(context.Background(), log)
	FromContext(ctx).Info("hello")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if rec["request_id"] != "req_1" {
		t.Errorf("request_id not propagated: %v", rec)
	}

	if FromContext(context.Background()) == nil {
		t.Error("FromContext must never return nil")
	}
}
