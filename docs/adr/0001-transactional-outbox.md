# 0001. Publish through a transactional outbox

Status: Accepted

## Context

The gateway must both persist the accepted request and make it visible to downstream consumers. Writing to Postgres and
then calling the Kafka producer from the same request handler has a window where the database commit succeeds and the
publish does not (process crash, broker unavailable, timeout). The reverse order has the opposite problem. Neither
Kafka transactions nor the idempotent producer cover a transaction that spans a database.

## Options

1. Dual write: commit to Postgres, then publish. Loses events on crash between the two steps.
2. Publish first, then commit. Produces events for requests that were never accepted.
3. Transactional outbox: write the event into an `outbox_events` row in the same transaction as the business write; a
   separate relay publishes rows and marks them.
4. Change data capture (Debezium) on the outbox table. Same guarantee as 3 with less polling, but a much heavier
   operational footprint for a reference project.

## Decision

Option 3. The gateway's `202` is returned only after the transaction that contains the outbox row commits, so an
accepted request is a durable event by construction. The relay (`services/relay-java`) is the only publisher.

## Consequences

- Publication is at-least-once. The relay can publish a row and crash before marking it `PUBLISHED`; the row is
  published again after its lease expires. Deduplication therefore lives downstream (see ADR 0002).
- Latency from acceptance to publication is bounded below by the relay poll interval (default 1 s).
- The outbox table grows and needs a retention job; the relay deletes `PUBLISHED` rows older than `relay.retention`.
