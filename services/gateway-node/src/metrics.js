import client from 'prom-client';

export function createMetrics() {
  const registry = new client.Registry();
  client.collectDefaultMetrics({ register: registry });
  const httpDuration = new client.Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request duration in seconds',
    labelNames: ['method', 'route', 'status'],
    registers: [registry],
  });
  const replays = new client.Counter({
    name: 'idempotency_replays_total',
    help: 'Replayed idempotent requests',
    labelNames: ['source'],
    registers: [registry],
  });
  const conflicts = new client.Counter({
    name: 'idempotency_conflicts_total',
    help: 'Idempotency key reused with a different payload',
    registers: [registry],
  });
  const accepted = new client.Counter({
    name: 'events_accepted_total',
    help: 'New events accepted into the outbox',
    registers: [registry],
  });
  for (const source of ['redis', 'postgres']) replays.inc({ source }, 0);
  return { registry, httpDuration, replays, conflicts, accepted };
}
