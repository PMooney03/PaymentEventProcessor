CREATE TABLE webhook_endpoints (
    id          UUID        PRIMARY KEY,
    url         TEXT        NOT NULL,
    secret      TEXT,
    active      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX webhook_endpoints_active_idx ON webhook_endpoints (active) WHERE active;
