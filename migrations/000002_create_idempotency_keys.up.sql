-- One row per Idempotency-Key. The primary key is the concurrency arbiter:
-- two concurrent requests with the same key race on INSERT and exactly one
-- wins. The loser reads the winner's stored response (or sees it in-flight).
CREATE TABLE idempotency_keys (
    key             TEXT        PRIMARY KEY,
    request_hash    TEXT        NOT NULL,
    event_id        UUID        REFERENCES events (id) ON DELETE SET NULL,
    response_status INTEGER,
    response_body   JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);
