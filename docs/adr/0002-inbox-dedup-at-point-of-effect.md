# 0002. Deduplicate in the consumer with an inbox table inside the effect transaction

Status: Accepted

## Context

ADR 0001 makes delivery at-least-once. Kafka can also redeliver a record when a consumer crashes after processing but
before committing its offset. The business effect must still happen exactly once.

## Options

1. Kafka exactly-once semantics (`read_process_write` transactions). Covers Kafka-to-Kafka only; the effect here is a
   Postgres write.
2. Remember processed ids in the consumer's memory or in Redis. Lost on restart or on Redis failure; not atomic with
   the effect.
3. Inbox table: `inbox_events(event_id PRIMARY KEY)`. Insert the inbox row and apply the effect in one Postgres
   transaction; commit the Kafka offset only after that transaction commits.

## Decision

Option 3. The primary key on `inbox_events` is the deduplication mechanism. A redelivered `event_id` fails the insert
with a unique violation; the consumer records a `DUPLICATE` attempt and acknowledges the record without touching
`business_state`.

## Consequences

- The guarantee holds only if every effect is written inside the same transaction as the inbox row. An effect that
  calls an external system is outside this guarantee and would need its own idempotency.
- `inbox_events` grows with every distinct event. It is not cleaned up here; a production deployment would prune rows
  older than the maximum redelivery window.
- Ordering is not guaranteed and not needed: the effect is keyed by `event_id`, not by sequence.
