# Changelog

## [0.1.0]

Initial public release.

- Node.js gateway with `Idempotency-Key` handling, request-hash conflict detection and a Redis replay cache.
- PostgreSQL transactional outbox written in the same transaction as the idempotency record.
- Java relay publishing to Kafka/Redpanda with leasing, backoff with jitter and attempt logging.
- Java consumer with inbox deduplication and transactional business effect.
- Docker Compose environment, Python load generator, and documented failure scenarios.
