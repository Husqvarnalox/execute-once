import { randomUUID } from 'node:crypto';
import express from 'express';
import { acceptEvent } from './idempotency.js';
import { validateEventBody, validateIdempotencyKey } from './validation.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const REQUEST_ID_RE = /^[\x21-\x7e]{1,128}$/;

const errorBody = (error, message, details) => (details ? { error, message, details } : { error, message });

export function createApp({ pg, redis, log, metrics, config }) {
  const app = express();
  app.disable('x-powered-by');

  app.use((req, res, next) => {
    const incoming = req.get('X-Request-Id');
    req.id = incoming && REQUEST_ID_RE.test(incoming) ? incoming : randomUUID();
    req.log = log.child({ requestId: req.id });
    res.set('X-Request-Id', req.id);
    const start = process.hrtime.bigint();
    res.on('finish', () => {
      const seconds = Number(process.hrtime.bigint() - start) / 1e9;
      const route = req.route ? `${req.baseUrl}${req.route.path}` : 'unmatched';
      metrics.httpDuration.observe({ method: req.method, route, status: String(res.statusCode) }, seconds);
    });
    next();
  });

  app.use(express.json({ limit: config.bodyLimit, strict: true }));

  app.get('/health', (_req, res) => {
    res.json({ status: 'ok' });
  });

  app.get('/ready', async (req, res) => {
    try {
      await pg.query('SELECT 1');
      res.json({ status: 'ready' });
    } catch (err) {
      req.log.error({ err }, 'readiness check failed');
      res.status(503).json({ status: 'unavailable' });
    }
  });

  app.get('/metrics', async (_req, res) => {
    res.set('Content-Type', metrics.registry.contentType);
    res.send(await metrics.registry.metrics());
  });

  app.post('/api/events', async (req, res) => {
    const idemKey = req.get('Idempotency-Key');
    const keyProblem = validateIdempotencyKey(idemKey);
    if (keyProblem) return res.status(400).json(errorBody(keyProblem.error, keyProblem.message));

    const details = validateEventBody(req.body);
    if (details.length) {
      return res.status(400).json(errorBody('validation_error', 'Request body is invalid', details));
    }

    const result = await acceptEvent(
      { pg, redis, log: req.log, metrics, config },
      { idemKey, body: req.body }
    );
    if (result.replayed) res.set('Idempotency-Replayed', 'true');
    res.status(result.status).json(result.body);
  });

  app.get('/api/events/:eventId', async (req, res) => {
    const { eventId } = req.params;
    const notFound = () => res.status(404).json(errorBody('not_found', 'Event not found'));
    if (!UUID_RE.test(eventId)) return notFound();

    const { rows } = await pg.query(
      `SELECT o.event_id, o.status AS outbox_status, o.created_at, o.published_at,
              i.received_at, b.created_at AS applied_at
         FROM outbox_events o
         LEFT JOIN inbox_events i ON i.event_id = o.event_id
         LEFT JOIN business_state b ON b.event_id = o.event_id
        WHERE o.event_id = $1`,
      [eventId]
    );
    if (!rows.length) return notFound();
    const r = rows[0];
    let status = 'pending';
    if (r.applied_at) status = 'applied';
    else if (r.outbox_status === 'FAILED') status = 'failed';
    else if (r.outbox_status === 'PUBLISHED') status = 'published';
    res.json({
      event_id: r.event_id,
      status,
      created_at: r.created_at,
      published_at: r.published_at,
      received_at: r.received_at,
      applied_at: r.applied_at,
    });
  });

  app.use((_req, res) => {
    res.status(404).json(errorBody('not_found', 'Route not found'));
  });

  app.use((err, req, res, _next) => {
    if (err?.type === 'entity.parse.failed') {
      return res.status(400).json(errorBody('validation_error', 'Malformed JSON body'));
    }
    if (err?.type === 'entity.too.large') {
      return res.status(413).json(errorBody('validation_error', 'Request body too large'));
    }
    if (err?.status >= 400 && err?.status < 500 && err?.expose) {
      return res.status(err.status).json(errorBody('validation_error', 'Invalid request'));
    }
    (req.log ?? log).error({ err }, 'unhandled error');
    if (res.headersSent) return res.end();
    res.status(500).json(errorBody('internal_error', 'Internal server error'));
  });

  return app;
}
