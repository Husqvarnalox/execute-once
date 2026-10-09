# 0003. Idempotency keys are locked rows with a canonical request hash

Status: Accepted

## Context

Clients retry on timeouts. Two retries of the same request can arrive concurrently, and a client can mistakenly reuse a
key for a different request. Both cases must be handled inside the gateway.

## Options

1. Check-then-insert (`SELECT`, then `INSERT`). Races between two first-time requests with the same key.
2. Redis `SET NX` as the lock. Fails when Redis is unavailable and is not atomic with the Postgres write.
3. `INSERT ... ON CONFLICT DO NOTHING` followed by `SELECT ... FOR UPDATE` on the key row, inside the same transaction
   as the outbox write. The loser of the race blocks on the row lock, then sees the winner's completed state.

## Decision

Option 3, with the request body hashed in canonical form (object keys sorted recursively) so that
`{"a":1,"b":2}` and `{"b":2,"a":1}` are the same request. A key reused with a different hash returns `409`.
Replayed responses carry the `Idempotency-Replayed: true` header so clients can tell a replay from a fresh accept.

## Consequences

- Concurrent duplicates serialize on a row lock rather than failing. Under a burst of retries for one key the
  gateway's connection pool can fill with waiters; the pool size bounds that.
- Redis holds the response as a cache only. A cache hit still enforces the hash check; a miss or a Redis error goes
  to Postgres (see ADR 0005).
- Keys are retained for `relay.retention` (default 7 days) and then removed by the cleanup job. A retry after that
  window creates a new event, which is the documented contract of an idempotency window.
