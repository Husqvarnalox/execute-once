# Architecture decision records

Short records of the decisions that shape this system. Each one states the problem, the options that were
considered, the choice, and what it costs. New decisions get a new file; superseded ones are marked, not deleted.

| ID | Decision | Status |
|---|---|---|
| [0001](0001-transactional-outbox.md) | Publish through a transactional outbox, not from the request handler | Accepted |
| [0002](0002-inbox-dedup-at-point-of-effect.md) | Deduplicate in the consumer with an inbox table inside the effect transaction | Accepted |
| [0003](0003-idempotency-key-lock-and-hash.md) | Idempotency keys are locked rows with a canonical request hash | Accepted |
| [0004](0004-lease-based-claiming.md) | Relay claims rows with `SKIP LOCKED` and a time lease instead of a status flag | Accepted |
| [0005](0005-redis-fails-open.md) | Redis is a cache and a limiter only; every Redis failure fails open | Accepted |
| [0006](0006-dead-letter-topic.md) | Poison and exhausted records go to a dead-letter topic with a terminal attempt row | Accepted |
