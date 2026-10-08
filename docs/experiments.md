# Experiments

## Load profiles (`scripts/python-bench`)
- `normal`: N requests with unique `Idempotency-Key`s.
- `dup30`: 30% of requests reuse an earlier key.

```bash
docker compose run --rm bench python run.py --profile dup30 --total 500
```

## Failure scenarios (manual, run under load)
1. **Consumer down:** `docker compose stop consumer`, wait 60 s, `docker compose start consumer`. The backlog drains
   without duplicate effects.
2. **Relay restart mid-batch:** `docker compose restart relay` under load. Events may be published twice; the inbox
   absorbs the duplicate.
3. **Redis down:** `docker compose stop redis`. The gateway falls back to Postgres; rate limiters allow traffic.
4. **Kafka down:** `docker compose stop redpanda`, send events, restart. Outbox rows retry with backoff and are
   published once Kafka returns (within `max-attempts`).
5. **Consumer crash before ack:** kill the consumer during processing; the record is redelivered and deduplicated.

## Invariant checks
```sql
-- no duplicate effects (expect 0 rows)
SELECT event_id, count(*) FROM business_state GROUP BY event_id HAVING count(*) > 1;

-- no stuck events (expect 0 in steady state)
SELECT count(*) FROM outbox_events
WHERE status = 'PENDING' AND next_attempt_at < now() - interval '5 minutes';

-- inbox rows without an applied effect (expect 0)
SELECT count(*) FROM inbox_events ie LEFT JOIN business_state b USING (event_id) WHERE b.event_id IS NULL;

-- accepted but never applied (expect 0 once drained)
SELECT count(*) FROM outbox_events o LEFT JOIN business_state b USING (event_id)
WHERE o.status = 'PUBLISHED' AND b.event_id IS NULL;
```
