# Практически однократная обработка (Node + Java + Kafka + Postgres + Redis)

## Как запустить
```bash
docker compose build
docker compose up -d
```
Сервисы:
- Gateway: http://localhost:3000/api/events (header `Idempotency-Key` обязателен)
- Postgres: localhost:5432 (app/app)
- Redis: localhost:6379
- Kafka/Redpanda: localhost:9092 (topic `events` создаётся при первой отправке)

## Быстрый тест
```bash
# Отправить событие
curl -X POST http://localhost:3000/api/events \
  -H 'Idempotency-Key: test-1' \
  -H 'Content-Type: application/json' \
  -d '{"amount":10,"note":"demo"}'

# Повтор с тем же ключом вернёт тот же ответ без дубля
```

## Нагрузочные профили
В контейнере `bench` (или локально):
```bash
python scripts/python-bench/run.py --profile normal --total 200
python scripts/python-bench/run.py --profile dup30 --total 500
```

## Где читать про детали
- Архитектура и инварианты: `docs/architecture.md`
- Эксперименты и сценарии отказов: `docs/experiments.md`
- Шаблон результатов: `docs/results.md`
- Схема БД: `migrations/001_init.sql`

## Сервисы
- `services/gateway-node`: Node.js intake + idempotency + outbox запись.
- `services/relay-java`: Spring Boot сервис для доставки outbox → Kafka с ретраями и rate limiting.
- `services/consumer-java`: Spring Boot consumer с inbox-дедупом и транзакцией side-effect.
- `scripts/python-bench`: лёгкий генератор нагрузки и дублей.
