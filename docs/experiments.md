# Experiments

The automated coverage lives in the test suites and in the `e2e` CI job. This page is for drills against a running
stack: reproducing a failure by hand and confirming the invariants with SQL.

## Load profiles

```bash
make up
make bench PROFILE=normal TOTAL=1000      # unique keys
make bench PROFILE=dup30  TOTAL=1000      # 30% of requests reuse an earlier key
make bench PROFILE=burst  TOTAL=2000      # unique keys at 200 concurrent requests
```

Every run ends with a verification phase: it waits until every accepted `event_id` has a `business_state` row,
reports end-to-end latency (`business_state.created_at - outbox_events.created_at`) and prints each invariant as
PASS/FAIL. The exit code is non-zero on any failure. Add `--report out.json` for a machine-readable record;
[results.md](results.md) is produced from those reports.

## Failure drills

Run a profile in one terminal and the drill in another. After the drill finishes, run the invariant queries below.

| Drill | Commands | Expected |
|---|---|---|
| Consumer down | `docker compose stop consumer`, wait 60 s, `docker compose start consumer` | Backlog drains; zero duplicate effects; `inbox_duplicate_total` may rise, `business_state` does not. |
| Relay restart mid-batch | `docker compose restart relay` while the profile runs | Some events published twice (visible as `DUPLICATE` attempts); exactly one effect each. |
| Redis down | `docker compose stop redis` | Gateway keeps returning `202` with replays served from Postgres; `idempotency_replays_total{source="postgres"}` rises; relay and consumer log a limiter warning and continue. |
| Kafka down | `docker compose stop redpanda`, send requests, `docker compose start redpanda` | Rows accumulate `ERROR` attempts with growing backoff; all are `PUBLISHED` once the broker is back, as long as the outage is shorter than the backoff budget. |
| Consumer crash before ack | `docker compose kill -s SIGKILL consumer` during load, then `start` | Last uncommitted records are redelivered and recorded as `DUPLICATE`. |
| Poison record | produce a record with a non-UUID key: `docker compose exec redpanda rpk topic produce events -k not-a-uuid` and type `{}` | Record appears on `events.dlq`; a `DEAD_LETTERED` row exists in `consumer_attempts`; the partition keeps moving. |

## Invariant queries

```sql
-- 1. no duplicate effects (expect 0 rows)
SELECT event_id, count(*) FROM business_state GROUP BY event_id HAVING count(*) > 1;

-- 2. no inbox row without an effect (expect 0)
SELECT count(*) FROM inbox_events ie
  LEFT JOIN business_state b USING (event_id) WHERE b.event_id IS NULL;

-- 3. nothing published but never applied, once the stack is idle (expect 0)
SELECT count(*) FROM outbox_events o
  LEFT JOIN business_state b USING (event_id)
 WHERE o.status = 'PUBLISHED' AND b.event_id IS NULL;

-- 4. no stuck rows: pending past their lease for more than five minutes (expect 0)
SELECT count(*) FROM outbox_events
 WHERE status = 'PENDING' AND next_attempt_at < now() - interval '5 minutes';

-- 5. one outbox row per idempotency key (expect 0 rows)
SELECT idem_key, count(*) FROM outbox_events GROUP BY idem_key HAVING count(*) > 1;

-- 6. terminal outcomes for everything the consumer saw
SELECT status, count(*) FROM consumer_attempts GROUP BY status;

-- 7. relay retry profile
SELECT attempt, status, count(*) FROM relay_attempts GROUP BY attempt, status ORDER BY attempt;
```

Run them with:

```bash
docker compose exec postgres psql -U app -d app
```
