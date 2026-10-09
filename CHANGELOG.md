# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses semantic versioning.

## [Unreleased]

### Added
- Supply-chain guard workflow `a10-guard` (forged-commit forensics, fake fonts, padded build configs, lifecycle scripts, VS Code auto-run tasks), branch and tag rulesets, SHA-pinned actions, CODEOWNERS for executable surfaces, IOC list in SECURITY.md.
- Unit and Testcontainers integration tests for the gateway, relay and consumer; CI runs them on every push.
- End-to-end CI job: Compose stack, `dup30` and `normal` load profiles, invariant verification.
- Dead-letter topic (`events.dlq`) for poison and exhausted records, with a terminal `DEAD_LETTERED` attempt row.
- Outbox and idempotency-key retention job in the relay (`relay.retention`, default 7 days).
- Prometheus metrics in all three services; Spring Boot Actuator health and readiness probes.
- `GET /api/events/:id` to inspect the lifecycle of an accepted event.
- `Idempotency-Replayed` response header and `X-Request-Id` propagation.
- Bench verification phase: end-to-end latency and invariant checks against Postgres, JSON reports.
- Architecture decision records in `docs/adr/`, Makefile, Maven wrapper, Dependabot, release workflow publishing
  images to GHCR.

### Changed
- Request hash uses canonical JSON (sorted keys), so key order no longer causes a false `409`.
- Redis clients fail fast; a Redis outage no longer stalls requests before falling back to Postgres.
- Relay: configurable lease, exponential backoff with full jitter capped at `relay.max-backoff`, batch claim with a
  single lease update, correct attempt accounting. A poll now drains the outbox in batches instead of publishing at
  most `batch-size` rows per interval, and rows held back by the rate limiter are released immediately instead of
  waiting for the lease to expire.
- Consumer: `BigDecimal` amounts, explicit topic declaration, Spring-managed listener configuration.
- Java services moved to Java 21, Spring Boot 3.5, package `dev.executeonce`; Lombok removed.
- Pinned container images; Compose waits on health checks for every service.

### Fixed
- Consumer duplicate handling could never record a `DUPLICATE` row: catching `DuplicateKeyException` inside a
  Postgres transaction leaves the transaction aborted, so the follow-up insert failed and the record was retried. The
  inbox insert now uses `ON CONFLICT DO NOTHING`.
- Actuator endpoints were configured but the dependency was missing, so no metrics were actually exposed.
- Relay recorded the pre-increment attempt number on success.
- Redis commands queued indefinitely while Redis was down, so the "fail open" fallback never ran.
- Host ports in Compose are now overridable (`POSTGRES_PORT`, `REDIS_PORT`, `KAFKA_PORT`, `GATEWAY_PORT`, ...).

## [0.1.0] - 2026-10-08

Initial public release.

- Node.js gateway with `Idempotency-Key` handling, request-hash conflict detection and a Redis replay cache.
- PostgreSQL transactional outbox written in the same transaction as the idempotency record.
- Java relay publishing to Kafka/Redpanda with leasing, backoff with jitter and attempt logging.
- Java consumer with inbox deduplication and transactional business effect.
- Docker Compose environment, Python load generator, and documented failure scenarios.
