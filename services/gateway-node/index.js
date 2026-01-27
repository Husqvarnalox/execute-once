import express from 'express';
import { Pool } from 'pg';
import Redis from 'ioredis';
import crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import pino from 'pino';

const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const IDEM_TTL_SECONDS = parseInt(process.env.IDEM_TTL_SECONDS || '3600', 10);
const IDEM_PROGRESS_TTL_SECONDS = parseInt(process.env.IDEM_PROGRESS_TTL_SECONDS || '30', 10);
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

const pool = new Pool({
  host: process.env.PGHOST || '127.0.0.1',
  port: Number(process.env.PGPORT || 5432),
  user: process.env.PGUSER || 'app',
  password: process.env.PGPASSWORD || 'app',
  database: process.env.PGDATABASE || 'app',
  max: 20,
});

const redis = new Redis(REDIS_URL);
const log = pino({ level: LOG_LEVEL });

const app = express();
app.use(express.json());

function requestHash(body) {
  return crypto.createHash('sha256').update(JSON.stringify(body)).digest('hex');
}

app.get('/health', async (_req, res) => {
  res.json({ status: 'ok' });
});

app.post('/api/events', async (req, res) => {
  const idemKey = req.header('Idempotency-Key');
  if (!idemKey) {
    return res.status(400).json({ error: 'Missing Idempotency-Key header' });
  }

  const payload = req.body || {};
  const hash = requestHash(payload);
  const cacheKey = `idem:${idemKey}`;

  try {
    const cached = await redis.get(cacheKey);
    if (cached) {
      log.debug({ idemKey }, 'hit redis cache');
      return res.json(JSON.parse(cached));
    }
  } catch (err) {
    log.warn({ err }, 'redis unavailable, fallback to postgres');
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const lock = await client.query(
      'SELECT idem_key, request_hash, status, response_body FROM idempotency_keys WHERE idem_key = $1 FOR UPDATE',
      [idemKey]
    );

    if (lock.rowCount > 0) {
      const row = lock.rows[0];
      if (row.request_hash !== hash) {
        await client.query('ROLLBACK');
        return res.status(409).json({ error: 'Idempotency key reused with different payload' });
      }

      if (row.status === 'COMPLETED') {
        await client.query('COMMIT');
        const response = row.response_body;
        try {
          await redis.set(cacheKey, JSON.stringify(response), 'EX', IDEM_TTL_SECONDS);
        } catch (err) {
          log.warn({ err }, 'redis set failed for completed replay');
        }
        return res.json(response);
      }

      if (row.status === 'IN_PROGRESS') {
        await client.query('ROLLBACK');
        try {
          await redis.set(cacheKey, JSON.stringify({ status: 'processing' }), 'EX', IDEM_PROGRESS_TTL_SECONDS);
        } catch (err) {
          log.warn({ err }, 'redis set failed for in-progress');
        }
        return res.status(202).json({ status: 'processing' });
      }
    } else {
      await client.query(
        'INSERT INTO idempotency_keys(idem_key, request_hash, status) VALUES ($1, $2, $3)',
        [idemKey, hash, 'IN_PROGRESS']
      );
    }

    const eventId = uuidv4();
    const eventPayload = payload;
    await client.query(
      'INSERT INTO outbox_events(event_id, idem_key, event_type, payload, status) VALUES ($1,$2,$3,$4,$5)',
      [eventId, idemKey, 'demo.event', eventPayload, 'PENDING']
    );

    const responseBody = { event_id: eventId, status: 'accepted' };

    await client.query(
      'UPDATE idempotency_keys SET status=$2, response_body=$3 WHERE idem_key=$1',
      [idemKey, 'COMPLETED', responseBody]
    );

    await client.query('COMMIT');

    try {
      await redis.set(cacheKey, JSON.stringify(responseBody), 'EX', IDEM_TTL_SECONDS);
    } catch (err) {
      log.warn({ err }, 'redis set failed after commit');
    }

    res.status(202).json(responseBody);
  } catch (err) {
    await client.query('ROLLBACK');
    log.error({ err }, 'failed to process request');
    try {
      await client.query('UPDATE idempotency_keys SET status=$2 WHERE idem_key=$1', [idemKey, 'FAILED']);
    } catch (_) {}
    res.status(500).json({ error: 'internal_error' });
  } finally {
    client.release();
  }
});

app.listen(PORT, () => {
  log.info(`Gateway listening on ${PORT}`);
});
