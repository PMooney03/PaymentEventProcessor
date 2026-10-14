CREATE TABLE events (
    id              UUID        PRIMARY KEY,
    type            TEXT        NOT NULL,
    payment_id      TEXT        NOT NULL,
    amount          BIGINT      NOT NULL CHECK (amount >= 0),
    currency        CHAR(3)     NOT NULL,
    customer_id     TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'processing', 'succeeded', 'failed')),
    retry_count     INTEGER     NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Worker claim query: WHERE status = 'pending' AND next_attempt_at <= now() ORDER BY created_at
CREATE INDEX events_claim_idx ON events (status, next_attempt_at, created_at);

-- Listing / filtering
CREATE INDEX events_type_idx ON events (type);
CREATE INDEX events_payment_id_idx ON events (payment_id);
CREATE INDEX events_created_at_idx ON events (created_at DESC, id DESC);
