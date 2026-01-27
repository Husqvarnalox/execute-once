# Сценарии экспериментов

## Профили нагрузки (используйте `scripts/python-bench`)
- `normal`: N уникальных запросов с уникальными `Idempotency-Key`.
- `dup30`: 30% запросов — повтор тех же `Idempotency-Key` (проверка idem + redis cache).

Пример: `python run.py --profile dup30 --total 500` (в контейнере bench — параметры по умолчанию).

## Сценарии отказов (ручные действия + нагрузка)
1. **Lost consumer reply**: во время обработки индуцировать таймаут (например, остановить сеть consumer) → повторная доставка, эффект не дублируется.
2. **Consumer down 60s**: `docker compose stop consumer` на 60s, затем `start` → backlog Kafka догоняется, без дублей.
3. **Relay restart mid-batch**: `docker compose restart relay` под нагрузкой → возможна повторная публикация в Kafka, но inbox гасит дубль.
4. **Postgres slowdown**: включить `pg_sleep` через `tc`/`pg_stat_statements` или уменьшить resources → проверить рост latency и rate-limit hit'ы.

## Метрики, которые собираем
- Корректность: `duplicate_effects`, `lost_events`, `applied_count`, `failed_count`.
- Производительность: outbox backlog (`SELECT count(*) WHERE status='PENDING'`), consumer lag (Kafka), e2e latency (gateway accepted → consumer applied), распределение попыток relay, rate-limit hits.

## Проверка инвариантов (ручные SQL)
- Дублей нет: `SELECT event_id, count(*) FROM business_state GROUP BY event_id HAVING count(*)>1;` → пусто.
- Потерь нет: `SELECT count(*) FROM outbox_events WHERE status='PENDING' AND next_attempt_at < now() - interval '5 minutes';` → 0 в steady-state.
- Соответствие applied vs inbox: `SELECT count(*) FROM inbox_events ie LEFT JOIN business_state b USING(event_id) WHERE b.event_id IS NULL;` → 0.
