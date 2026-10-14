-- One row per delivery attempt. A failed attempt that still has retries left
-- schedules a new 'pending' row with attempt_number + 1, so the table doubles
-- as the delivery queue and the audit log.
CREATE TABLE webhook_attempts (
    id              UUID        PRIMARY KEY,
    endpoint_id     UUID        NOT NULL REFERENCES webhook_endpoints (id) ON DELETE CASCADE,
    event_id        UUID        NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    attempt_number  INTEGER     NOT NULL CHECK (attempt_number >= 1),
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'delivering', 'succeeded', 'failed')),
    response_status INTEGER,
    error           TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempted_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (endpoint_id, event_id, attempt_number)
);

-- Dispatcher claim query
CREATE INDEX webhook_attempts_claim_idx ON webhook_attempts (status, next_attempt_at);

-- Lookup attempts for an event
CREATE INDEX webhook_attempts_event_idx ON webhook_attempts (event_id, attempt_number);
