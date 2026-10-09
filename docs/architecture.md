# Architecture

This document describes what each component does and which invariants it is responsible for. The reasoning behind
the design lives in the [ADRs](adr/README.md).

## Data flow

```
client ──POST /api/events──▶ gateway ──tx──▶ postgres (idempotency_keys, outbox_events)
                                                   │
                           relay ◀──SKIP LOCKED────┘        relay ──acks=all──▶ redpanda:events
                                                                                      │
                           consumer ◀─────────────────────────────────────────────────┘
                           consumer ──tx──▶ postgres (inbox_events, business_state, consumer_attempts)
                           consumer ──poison / exhausted──▶ redpanda:events.dlq
```

### Gateway (`services/gateway-node`, Node 22, Express 5)

- Validates `Idempotency-Key` (1–128 printable ASCII chars) and the body (`amount` ≥ 0, optional `note` ≤ 256
  chars, no unknown fields). Bodies are limited to `BODY_LIMIT` (16 KiB).
- Computes the request hash over canonical JSON (recursively sorted keys).
- Reads the Redis replay cache first. A cache hit still enforces the hash check. A cache miss or Redis error falls
  through to Postgres.
- Postgres intake, one transaction: `INSERT ... ON CONFLICT DO NOTHING` → `SELECT ... FOR UPDATE` →
  (if not yet completed) insert `outbox_events` → update the key to `COMPLETED` with the stored response. The `202`
  is sent only after `COMMIT`.
- `GET /api/events/:id` joins outbox, inbox and business state to report `pending | published | applied | failed`.
- `/health`, `/ready` (runs `SELECT 1`), `/metrics` (Prometheus). Every response carries `X-Request-Id`; replays carry
  `Idempotency-Replayed: true`.
- Shutdown: stop accepting, drain in-flight requests up to `SHUTDOWN_TIMEOUT_MS`, close the pool, quit Redis.

### Relay (`services/relay-java`, Spring Boot 3.5, Java 21)

- Every `relay.poll-interval`: in one short transaction, `SELECT ... FOR UPDATE SKIP LOCKED LIMIT batch-size` on
  `PENDING` rows whose `next_attempt_at <= now()`, then push `next_attempt_at` forward by `relay.lease` for the whole
  batch. Commit. No Kafka call happens inside that transaction. A tick keeps claiming batches until one comes back
  short, so throughput is not capped at `batch-size / poll-interval`.
- Sends every claimed row to `relay.topic` (key = `event_id`, value = payload JSON) with `acks=all` and
  `enable.idempotence=true`, then waits for each acknowledgement.
- Success: `status = PUBLISHED`, `published_at = now()`, `attempt = attempt + 1`, a `SUCCESS` row in
  `relay_attempts`.
- Failure: `attempt + 1`; if that exceeds `relay.max-attempts` the row becomes `FAILED`, otherwise
  `next_attempt_at = now() + backoff(attempt)` where backoff is exponential with full jitter, capped at
  `relay.max-backoff`. An `ERROR` row with the truncated message is written either way.
- Cleanup every `relay.cleanup-interval`: delete `PUBLISHED` rows older than `relay.retention` in batches, then
  idempotency keys older than `relay.retention` that no longer have outbox rows.
- Declares the `events` topic explicitly (`NewTopic` bean) so the system does not rely on broker auto-creation.
- Rate limiting: a Redis fixed window per second, advisory, fails open; `0` disables it and skips Redis. When the
  limiter denies, the unsent remainder of the batch has its lease released immediately so it is retried on the next
  tick rather than after `relay.lease`.

### Consumer (`services/consumer-java`, Spring Boot 3.5, Java 21)

- Manual acknowledgement, `enable.auto.commit=false`, `consumer.concurrency` listener threads.
- Per record: parse key as UUID and body as `{amount, note}`; anything malformed raises `PoisonRecordException`.
- One transaction: `INSERT inbox_events(event_id)`; on a unique violation write `consumer_attempts(DUPLICATE)` and
  stop; otherwise `INSERT business_state` (amount as `BigDecimal`) and `consumer_attempts(APPLIED)`. The offset is
  acknowledged after the transaction commits.
- Error handling: `PoisonRecordException` is not retryable and goes to `consumer.dead-letter-topic` immediately; any
  other exception is retried with exponential backoff up to `consumer.max-retries`, then dead-lettered. Before the
  record is published to the dead-letter topic a `consumer_attempts(DEAD_LETTERED)` row is written outside the failed
  transaction.

### Redis

Replay cache for the gateway and fixed-window rate limiter for relay and consumer. Never a source of truth. Clients are
configured to fail fast; every failure is logged and the operation proceeds as if Redis had no answer.

### PostgreSQL

Source of truth for idempotency, outbox, inbox and business state. Schema in `migrations/`, applied in file order by
the Postgres image on first start and by the integration tests.

## Tables

| Table | Written by | Purpose |
|---|---|---|
| `idempotency_keys(idem_key PK, request_hash, status, response_body, created_at, updated_at)` | gateway | Lock row and stored response per key. `status` is `IN_PROGRESS` only inside the intake transaction. |
| `outbox_events(event_id PK, idem_key FK, event_type, payload, status, attempt, next_attempt_at, created_at, published_at)` | gateway, relay | The event to publish and its delivery state: `PENDING → PUBLISHED | FAILED`. |
| `inbox_events(event_id PK, received_at)` | consumer | Deduplication record; the primary key is the mechanism. |
| `business_state(id, event_id UNIQUE, amount, note, created_at)` | consumer | The effect. `event_id UNIQUE` is a second line of defence. |
| `relay_attempts(event_id, attempt, status, error, created_at)` | relay | One row per publish attempt, `SUCCESS` or `ERROR`. |
| `consumer_attempts(event_id, status, error, created_at)` | consumer | One row per processing outcome: `APPLIED`, `DUPLICATE`, `DEAD_LETTERED`. |

Indexes that matter: a partial index on `outbox_events(next_attempt_at) WHERE status = 'PENDING'` for the relay
poll, a partial index on `published_at WHERE status = 'PUBLISHED'` for cleanup, and `idempotency_keys(created_at)`
for key retention.

## Invariants

1. **Idempotent intake.** For a given key, at most one `outbox_events` row exists, and every `202` for that key
   carries the same `event_id`.
2. **Durable acceptance.** A `202` implies the outbox row is committed.
3. **At-least-once publication.** Every `PENDING` row is either published within `max-attempts` or marked `FAILED`
   with a metric and an attempt log.
4. **Exactly-once effect.** For every `event_id` there is at most one `business_state` row, and every
   `inbox_events` row has a matching `business_state` row.
5. **Terminal state for everything.** No record is silently dropped: it ends `PUBLISHED`/`APPLIED`, `FAILED`, or
   `DEAD_LETTERED` with a row in the attempts table.

The bench's verification phase checks 1, 3 (no `FAILED` rows for its run), 4 and the drain of every event it sent.

Not guaranteed: ordering across events; delivery of events whose relay attempts are exhausted; automatic replay of
dead-lettered records.

## State machines

- Idempotency key: `IN_PROGRESS → COMPLETED` within one transaction. `FAILED` exists in the schema for a future
  asynchronous intake and is treated as retryable.
- Outbox: `PENDING → PUBLISHED | FAILED`. A lease does not change the status; it only moves `next_attempt_at`.
- Inbox: a row appears once. Repeats are `DUPLICATE` attempts, never a second row.

## Observability

- Gateway: `http_request_duration_seconds{method,route,status}`, `events_accepted_total`,
  `idempotency_replays_total{source}`, `idempotency_conflicts_total`, plus Node default metrics.
- Relay: `outbox_relay_backlog`, `outbox_relay_published_total`, `outbox_relay_failed_total{terminal}`,
  `outbox_relay_publish_seconds`.
- Consumer: `inbox_applied_total`, `inbox_duplicate_total`, `inbox_dead_lettered_total`, `inbox_process_seconds`,
  plus Spring Kafka consumer metrics (lag, fetch rates).
- Structured logs: pino in the gateway (with `requestId`), SLF4J/Logback in the Java services.
