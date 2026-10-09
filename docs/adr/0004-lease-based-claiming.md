# 0004. Relay claims rows with `SKIP LOCKED` and a time lease

Status: Accepted

## Context

Several relay instances may poll the same outbox table. A row must not be published twice concurrently, and a relay
that crashes mid-batch must not leave rows stuck forever.

## Options

1. Status flag `CLAIMED` set in a transaction. Simple, but a crash after claiming leaves rows stuck until a separate
   reaper runs.
2. Hold the row lock for the duration of the publish. Ties a database transaction to a network call to Kafka; long
   transactions, lock pile-ups, and connection exhaustion under broker slowness.
3. Claim with `SELECT ... FOR UPDATE SKIP LOCKED`, push `next_attempt_at` forward by a lease in the same short
   transaction, commit, then publish outside any transaction. If the relay dies, the lease expires and another
   instance picks the row up.

## Decision

Option 3 with `relay.lease` (default 15 s). The row stays `PENDING` during the lease; `PUBLISHED` is written only after
the broker acknowledges with `acks=all`.

## Consequences

- A publish that succeeds after the lease expires is a duplicate publish. This is accepted because ADR 0002 absorbs
  it; the lease is tuned so that it is rare, not impossible.
- The claim transaction is short and does not depend on broker latency.
- Retries use exponential backoff with full jitter, capped at `relay.max-backoff`, up to `relay.max-attempts`, after
  which the row is marked `FAILED` and surfaced through the `outbox_relay_failed_total{terminal="true"}` metric.
