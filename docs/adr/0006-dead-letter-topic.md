# 0006. Poison and exhausted records go to a dead-letter topic with a terminal attempt row

Status: Accepted

## Context

A consumer record can be unprocessable in two different ways: it is malformed (non-UUID key, invalid JSON, missing
amount) and will never succeed, or the database is temporarily unavailable and it will succeed later. Without a
terminal state, a malformed record either blocks the partition or is silently skipped after the error handler gives
up.

## Decision

- Malformed records raise `PoisonRecordException`, which is registered as not retryable: they go to the dead-letter
  topic (`consumer.dead-letter-topic`, default `events.dlq`) on the first failure.
- Other exceptions are retried with exponential backoff up to `consumer.max-retries`, then dead-lettered.
- Before publishing to the dead-letter topic the consumer records `consumer_attempts(status = 'DEAD_LETTERED')` with
  the error, outside the failed transaction, so the database has a terminal record for every event the system gave
  up on. The `inbox_dead_lettered_total` metric counts the same thing.

## Consequences

- The dead-letter topic has no automatic reprocessing. Replaying from it is an operator action.
- A record on the dead-letter topic never has an `inbox_events` row, so a later replay of the same `event_id` will
  be applied once, not treated as a duplicate.
