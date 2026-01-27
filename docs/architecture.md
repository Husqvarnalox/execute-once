# Практически однократная обработка (Node + Java + Kafka + Postgres + Redis)

## Потоки
- **Gateway (Node)**: HTTP intake, idem по `Idempotency-Key`, транзакция `idempotency_keys + outbox_events`, ответ `{event_id, accepted}`; горячий кэш ответов в Redis.
- **Outbox Relay (Java)**: читает `outbox_events` с `FOR UPDATE SKIP LOCKED`, проверяет rate limit в Redis, публикует в Kafka (`acks=all`, `enable.idempotence=true`), backoff+jitter, фиксирует `relay_attempts` и статусы `PUBLISHED/FAILED`.
- **Consumer (Java)**: rate-limit/семафоры через Redis, транзакция `inbox_events` (unique) + бизнес-эффект (`business_state`), только после commit — ACK offset; дубли гасит уникальный ключ inbox.
- **Kafka/Redpanda**: транспорт, партиционирование по `event_id/tenant`, хранение для replay.
- **Redis**: hot idem cache, rate limiting, опционально семафоры; не источник истины.
- **Postgres**: источник истины (idem/outbox/inbox/business/audit).
- **Python bench**: нагрузка, дубли, быстрые smoke-тесты инвариантов.

## Таблицы (см. `migrations/001_init.sql`)
- `idempotency_keys(idem_key, request_hash, status, response_body, timestamps)`
- `outbox_events(event_id, idem_key, event_type, payload, status, attempt, next_attempt_at, published_at)`
- `inbox_events(event_id, received_at)`
- `business_state(event_id, amount, note)`
- журналы `relay_attempts`, `consumer_attempts`

## Гарантии
1. Idempotent intake: повтор с тем же ключом → тот же ответ, без новой записи в outbox.
2. Durable event creation: `202 accepted` ⇒ событие записано в outbox.
3. At-least-once до Kafka (relay с ретраями и idempotent producer).
4. Exactly-once эффекта: inbox unique + транзакция side-effect.
5. No silent loss: каждое принятое событие в терминальном состоянии `APPLIED` или `FAILED`.

## Состояния
- Idempotency: `IN_PROGRESS → COMPLETED|FAILED`.
- Outbox: `PENDING → PUBLISHED | FAILED` (backoff по attempt).
- Inbox: запись появляется один раз; повторы не меняют состояние.

## Rate limiting
- Ключи Redis: `rl:gateway:<tenant>:<window>`, `rl:relay:global:<sec>`, `rl:consumer:<tenant>:<sec>`, `sem:consumer:<tenant>`.
- Алгоритм: token bucket / fixed window (реализация в коде — простое окно per-second).

## Инварианты
- Нет дублей эффекта по `event_id`.
- Нет потерь: принятое событие либо применено, либо прозрачно `FAILED`.
- Повторы доставки не изменяют бизнес-состояние.
- Рестарты компонентов сохраняют инварианты.

## Наблюдаемость
- Логи структурные (pino/slf4j).
- Метрики Prometheus из Spring Boot; Redis hit ratio/rl hits в логах.
- Основные KPI: backlog outbox, consumer lag, e2e latency, retries, rate-limit hits.
