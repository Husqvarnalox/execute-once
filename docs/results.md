# Results

Numbers from `scripts/python-bench` against the Compose stack, collected on 2026-10-10. They describe this
development setup, not a production deployment; the point is the invariant columns, not the absolute latency.

**Hardware.** Apple M5 Pro, 64 GB RAM, Docker Desktop 29.1 (linux/arm64 VM). Single-node Redpanda with
`--smp 1 --memory 1G`, single Postgres 16, one instance of each service with the default configuration
(`relay.poll-interval = 1s`, `relay.batch-size = 200`, `consumer.concurrency = 3`). Load generator running inside the
Compose network.

| Profile | Requests | Concurrency | Accepted | Replayed | Conflicts | req/s | HTTP p50 / p95 / p99 (ms) | e2e p50 / p95 / p99 (s) | Drain (s) | Duplicate effects | Lost | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| normal | 1000 | 50 | 1000 | 0 | 0 | 3166 | 14 / 26 / 29 | 1.18 / 1.28 / 1.30 | 2.0 | 0 | 0 | PASS |
| dup30 | 1000 | 50 | 690 | 310 | 0 | 3601 | 15 / 23 / 37 | 0.61 / 0.63 / 0.63 | 1.0 | 0 | 0 | PASS |
| burst | 2000 | 200 | 2000 | 0 | 0 | 4189 | 41 / 77 / 95 | 1.08 / 2.44 / 2.45 | 3.0 | 0 | 0 | PASS |
| burst + relay `SIGKILL` | 3000 | 200 | 3000 | 0 | 0 | 2694 | 69 / 113 / 162 | 3.78 / 9.34 / 9.38 | 9.1 | 0 | 0 | PASS |

Column notes:

- **Accepted / Replayed** are the gateway's `202` responses without and with `Idempotency-Replayed: true`. For
  `dup30`, 310 of 1000 requests reused a key; every one of them replayed the original `event_id` and none created a
  second outbox row.
- **HTTP** is the gateway round trip measured by the load generator. **e2e** is `business_state.created_at -
  outbox_events.created_at` per event, so it includes the relay poll interval (up to 1 s) and consumer processing.
- **Drain** is how long after the last request the verifier waited until every event had a `business_state` row.
- **Duplicate effects** is `SELECT event_id FROM business_state GROUP BY 1 HAVING count(*) > 1`; **Lost** is accepted
  events without a `business_state` row after drain. Both must be 0 for a PASS.

## Relay kill drill

The last row is the `burst` profile with `docker compose kill -s SIGKILL relay` about one second into the run,
followed by `docker compose start relay`. Rows that were claimed by the dead relay became visible again when their
15 s lease expired, which is where the higher e2e p95 comes from. Every event reached `business_state` once;
`consumer_attempts` shows only `APPLIED` rows for the run. The duplicate-publish window (publish acknowledged, process
killed before `PUBLISHED` is written) was not hit in this particular run, so the inbox deduplication path is covered
by the consumer integration test rather than by this drill.

## What the numbers do not show

- Multi-instance relay or consumer behaviour under contention.
- Broker or database failover.
- Throughput ceilings: the gateway was the measured component; nothing here was tuned, and Redpanda was deliberately
  constrained to one core.

Reproduce with `make up` followed by `make bench PROFILE=<name> TOTAL=<n>`; add `--report` for the JSON these tables
were built from.
