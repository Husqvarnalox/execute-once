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
      const entry = JSON.parse(cached);
      if (entry.hash !== hash) {
        return res.status(409).json({ error: 'Idempotency key reused with different payload' });
      }
      log.debug({ idemKey }, 'hit redis cache');
      return res.status(entry.status).json(entry.body);
    }
  } catch (err) {
    log.warn({ err }, 'redis unavailable, fallback to postgres');
  }

  const cacheResponse = async (status, body, ttl) => {
    try {
      await redis.set(cacheKey, JSON.stringify({ hash, status, body }), 'EX', ttl);
    } catch (err) {
      log.warn({ err }, 'redis set failed');
    }
  };

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // ON CONFLICT closes the race between two first-time requests with the same key:
    // the loser waits on the winner's row lock in the SELECT below instead of failing.
    await client.query(
      "INSERT INTO idempotency_keys(idem_key, request_hash, status) VALUES ($1, $2, 'IN_PROGRESS') ON CONFLICT (idem_key) DO NOTHING",
      [idemKey, hash]
    );
    const lock = await client.query(
      'SELECT request_hash, status, response_body FROM idempotency_keys WHERE idem_key = $1 FOR UPDATE',
      [idemKey]
    );
    const row = lock.rows[0];

    if (row.request_hash !== hash) {
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'Idempotency key reused with different payload' });
    }

    if (row.status === 'COMPLETED') {
      await client.query('COMMIT');
      await cacheResponse(202, row.response_body, IDEM_TTL_SECONDS);
      return res.status(202).json(row.response_body);
    }

    const eventId = uuidv4();
    await client.query(
      'INSERT INTO outbox_events(event_id, idem_key, event_type, payload, status) VALUES ($1,$2,$3,$4,$5)',
      [eventId, idemKey, 'demo.event', payload, 'PENDING']
    );

    const responseBody = { event_id: eventId, status: 'accepted' };
    await client.query(
      'UPDATE idempotency_keys SET status=$2, response_body=$3 WHERE idem_key=$1',
      [idemKey, 'COMPLETED', responseBody]
    );
    await client.query('COMMIT');

    await cacheResponse(202, responseBody, IDEM_TTL_SECONDS);
    res.status(202).json(responseBody);
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch (_) {}
    log.error({ err }, 'failed to process request');
    res.status(500).json({ error: 'internal_error' });
  } finally {
    client.release();
  }
});

const server = app.listen(PORT, () => {
  log.info(`Gateway listening on ${PORT}`);
});

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => {
    server.close(async () => {
      await pool.end().catch(() => {});
      redis.disconnect();
      process.exit(0);
    });
  });
}
