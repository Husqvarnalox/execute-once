import { randomUUID } from 'node:crypto';
import { requestHash } from './canonical.js';

const conflictResult = (source) => ({
  status: 409,
  body: {
    error: 'idempotency_key_conflict',
    message: 'Idempotency-Key was already used with a different payload',
  },
  replayed: false,
  conflict: true,
  source,
});

async function cacheRead(redis, log, cacheKey) {
  try {
    const raw = await redis.get(cacheKey);
    return raw ? JSON.parse(raw) : null;
  } catch (err) {
    log.warn({ err }, 'redis read failed, falling back to postgres');
    return null;
  }
}

async function cacheWrite(redis, log, cacheKey, entry, ttl) {
  try {
    await redis.set(cacheKey, JSON.stringify(entry), 'EX', ttl);
  } catch (err) {
    log.warn({ err }, 'redis write failed, ignoring');
  }
}

/**
 * One Postgres transaction:
 *   INSERT ... ON CONFLICT DO NOTHING  (closes the first-request race; the loser blocks on the
 *   winner's row), SELECT ... FOR UPDATE, outbox insert, UPDATE to COMPLETED.
 *
 * Row status handling:
 *  - COMPLETED: replay the stored response.
 *  - IN_PROGRESS: the row we just inserted in this transaction -> run intake.
 *  - FAILED: treated as retryable. We neither delete the row nor rewrite its hash (the hash was
 *    already verified equal above); we simply re-run intake for it inside this transaction
 *    (insert outbox row + mark COMPLETED), so the same key ends up with exactly one outbox row.
 */
async function intakeInPostgres(pg, { idemKey, body, hash, eventType }) {
  const client = await pg.connect();
  try {
    await client.query('BEGIN');
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
      return { kind: 'conflict' };
    }
    if (row.status === 'COMPLETED') {
      await client.query('COMMIT');
      return { kind: 'replay', body: row.response_body };
    }

    const eventId = randomUUID();
    await client.query(
      'INSERT INTO outbox_events(event_id, idem_key, event_type, payload, status) VALUES ($1,$2,$3,$4,$5)',
      [eventId, idemKey, eventType, body, 'PENDING']
    );
    const responseBody = { event_id: eventId, status: 'accepted' };
    await client.query('UPDATE idempotency_keys SET status=$2, response_body=$3 WHERE idem_key=$1', [
      idemKey,
      'COMPLETED',
      responseBody,
    ]);
    await client.query('COMMIT');
    return { kind: 'accepted', body: responseBody };
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // connection may already be broken; original error is what matters
    }
    throw err;
  } finally {
    client.release();
  }
}

/** Returns { status, body, replayed, source, conflict? }. Throws on Postgres failure. */
export async function acceptEvent({ pg, redis, log, metrics, config }, { idemKey, body }) {
  const hash = requestHash(body);
  const cacheKey = `idem:${idemKey}`;
  const ttl = config?.idemTtlSeconds ?? 3600;
  const eventType = config?.eventType ?? 'event.created';

  const cached = await cacheRead(redis, log, cacheKey);
  if (cached) {
    if (cached.hash !== hash) {
      metrics?.conflicts.inc();
      return conflictResult('redis');
    }
    metrics?.replays.inc({ source: 'redis' });
    return { status: cached.status, body: cached.body, replayed: true, source: 'redis' };
  }

  const result = await intakeInPostgres(pg, { idemKey, body, hash, eventType });
  if (result.kind === 'conflict') {
    metrics?.conflicts.inc();
    return conflictResult('postgres');
  }
  await cacheWrite(redis, log, cacheKey, { hash, status: 202, body: result.body }, ttl);
  if (result.kind === 'replay') {
    metrics?.replays.inc({ source: 'postgres' });
    return { status: 202, body: result.body, replayed: true, source: 'postgres' };
  }
  metrics?.accepted.inc();
  return { status: 202, body: result.body, replayed: false, source: 'postgres' };
}
