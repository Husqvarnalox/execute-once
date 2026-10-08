# Architecture

## Data flow
- **Gateway (Node.js):** HTTP intake keyed by `Idempotency-Key`. One transaction covers `idempotency_keys` and
  `outbox_events`. Response: `202 {event_id, status}`. A Redis cache (storing request hash, status and body) serves
  replays; Postgres remains authoritative.
- **Relay (Java):** claims `outbox_events` rows with `FOR UPDATE SKIP LOCKED`, leases them for 15 s, publishes to
  Kafka (`acks=all`, `enable.idempotence=true`, key = `event_id`), then marks `PUBLISHED`. Failures back off
  exponentially with jitter up to `max-attempts`, then `FAILED`. Every attempt is recorded in `relay_attempts`.
- **Consumer (Java):** manual ack. In one transaction it inserts `inbox_events(event_id)` and `business_state`;
  a unique-key violation on the inbox means a duplicate. The offset is acknowledged after commit.
- **Redis:** response cache and fixed-window rate limiting. Not a source of truth; all uses fail open.
- **PostgreSQL:** source of truth for idempotency, outbox, inbox and business state.

## Tables (`migrations/001_init.sql`)
- `idempotency_keys(idem_key, request_hash, status, response_body, timestamps)`
- `outbox_events(event_id, idem_key, event_type, payload, status, attempt, next_attempt_at, published_at)`
- `inbox_events(event_id, received_at)`
- `business_state(event_id UNIQUE, amount, note)`
- `relay_attempts`, `consumer_attempts` — attempt logs

## Guarantees
1. Idempotent intake: the same key and payload returns the original response and creates no new outbox row.
2. Durable acceptance: a `202` means the outbox row is committed.
3. At-least-once publication to Kafka.
4. Exactly-once business effect: `inbox_events` primary key plus a single transaction for the effect.

Not guaranteed: ordering across events, delivery of events whose relay attempts are exhausted (`FAILED`), or recovery
of consumer records skipped after the error handler gives up (see Limitations in the README).

## State machines
- Idempotency: `IN_PROGRESS → COMPLETED` (within one transaction, so `IN_PROGRESS` is not externally visible).
- Outbox: `PENDING → PUBLISHED | FAILED`.
- Inbox: a row appears once; repeats are logged as `DUPLICATE`.

## Rate limiting
Fixed one-second windows in Redis: `rl:relay:global:<epoch-second>` and `rl:consumer:global:<epoch-second>`. When
Redis is unreachable the limiters allow traffic.

## Observability
Structured logs (pino in the gateway, SLF4J in the Java services). The Java services expose Spring Boot Actuator
endpoints including Prometheus metrics. Useful signals: outbox backlog, consumer lag, retry counts.
