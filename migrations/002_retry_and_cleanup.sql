-- Idempotent follow-up to 001_init.sql.

DROP INDEX IF EXISTS idx_outbox_status_ready;

CREATE INDEX IF NOT EXISTS idx_outbox_pending_ready
    ON outbox_events(next_attempt_at) WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_outbox_published_at
    ON outbox_events(published_at) WHERE status = 'PUBLISHED';

CREATE INDEX IF NOT EXISTS idx_idempotency_created_at
    ON idempotency_keys(created_at);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'consumer_attempts_status_check') THEN
        ALTER TABLE consumer_attempts
            ADD CONSTRAINT consumer_attempts_status_check
            CHECK (status IN ('APPLIED','DUPLICATE','DEAD_LETTERED'));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'relay_attempts_status_check') THEN
        ALTER TABLE relay_attempts
            ADD CONSTRAINT relay_attempts_status_check
            CHECK (status IN ('SUCCESS','ERROR'));
    END IF;
END
$$;
