CREATE TABLE IF NOT EXISTS idempotency_keys (
    idem_key        text PRIMARY KEY,
    request_hash    text NOT NULL,
    status          text NOT NULL CHECK (status IN ('IN_PROGRESS','COMPLETED','FAILED')),
    response_body   jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_idem_status ON idempotency_keys(status);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id        uuid PRIMARY KEY,
    idem_key        text REFERENCES idempotency_keys(idem_key),
    event_type      text NOT NULL,
    payload         jsonb NOT NULL,
    status          text NOT NULL CHECK (status IN ('PENDING','PUBLISHED','FAILED')),
    attempt         int NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_ready ON outbox_events(status, next_attempt_at);

CREATE TABLE IF NOT EXISTS inbox_events (
    event_id    uuid PRIMARY KEY,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS business_state (
    id          bigserial PRIMARY KEY,
    event_id    uuid UNIQUE NOT NULL,
    amount      numeric(12,2) NOT NULL,
    note        text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS relay_attempts (
    id          bigserial PRIMARY KEY,
    event_id    uuid NOT NULL,
    attempt     int NOT NULL,
    status      text NOT NULL,
    error       text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS consumer_attempts (
    id          bigserial PRIMARY KEY,
    event_id    uuid NOT NULL,
    status      text NOT NULL,
    error       text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_idempotency_touch
BEFORE UPDATE ON idempotency_keys
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
