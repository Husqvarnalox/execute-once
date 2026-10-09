import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import request from 'supertest';
import pino from 'pino';
import pg from 'pg';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import { createApp } from '../../src/app.js';
import { createMetrics } from '../../src/metrics.js';
import { createRedis } from '../../src/clients.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const migrationsDir = path.resolve(here, '../../../../migrations');

describe('intake (postgres + redis testcontainers)', () => {
  let pgContainer;
  let redisContainer;
  let pool;
  let redis;
  let app;
  const log = pino({ level: 'silent' });

  const post = (key, body) => request(app).post('/api/events').set('Idempotency-Key', key).send(body);
  const outboxCount = async (key) =>
    Number((await pool.query('SELECT count(*) FROM outbox_events WHERE idem_key=$1', [key])).rows[0].count);

  before(
    async () => {
      [pgContainer, redisContainer] = await Promise.all([
        new PostgreSqlContainer('postgres:16-alpine').start(),
        new RedisContainer('redis:7-alpine').start(),
      ]);
      pool = new pg.Pool({ connectionString: pgContainer.getConnectionUri(), max: 30 });
      const files = (await readdir(migrationsDir)).filter((f) => f.endsWith('.sql')).sort();
      for (const f of files) await pool.query(await readFile(path.join(migrationsDir, f), 'utf8'));

      const config = {
        bodyLimit: '16kb',
        idemTtlSeconds: 60,
        eventType: 'event.created',
        redisUrl: redisContainer.getConnectionUrl(),
      };
      redis = createRedis(config, log);
      app = createApp({ pg: pool, redis, log, metrics: createMetrics(), config });
    },
    { timeout: 240000 }
  );

  after(
    async () => {
      redis?.disconnect();
      await pool?.end();
      await Promise.allSettled([pgContainer?.stop(), redisContainer?.stop()]);
    },
    { timeout: 60000 }
  );

  it('first request -> 202 and exactly one outbox row', async () => {
    const key = `k-${randomUUID()}`;
    const res = await post(key, { amount: 10, note: 'a' });
    assert.equal(res.status, 202);
    assert.equal(res.body.status, 'accepted');
    assert.equal(res.headers['idempotency-replayed'], undefined);
    assert.equal(await outboxCount(key), 1);
  });

  it('same request again -> same event_id, replayed, still one row', async () => {
    const key = `k-${randomUUID()}`;
    const first = await post(key, { amount: 10 });
    const second = await post(key, { amount: 10 });
    assert.equal(second.status, 202);
    assert.equal(second.body.event_id, first.body.event_id);
    assert.equal(second.headers['idempotency-replayed'], 'true');
    assert.equal(await outboxCount(key), 1);
  });

  it('replays from postgres when the redis cache is empty', async () => {
    const key = `k-${randomUUID()}`;
    const first = await post(key, { amount: 4 });
    await redis.del(`idem:${key}`);
    const second = await post(key, { amount: 4 });
    assert.equal(second.body.event_id, first.body.event_id);
    assert.equal(second.headers['idempotency-replayed'], 'true');
    assert.equal(await outboxCount(key), 1);
  });

  it('different payload -> 409', async () => {
    const key = `k-${randomUUID()}`;
    await post(key, { amount: 1 });
    const res = await post(key, { amount: 2 });
    assert.equal(res.status, 409);
    assert.equal(res.body.error, 'idempotency_key_conflict');
    // also via postgres only
    await redis.del(`idem:${key}`);
    const res2 = await post(key, { amount: 3 });
    assert.equal(res2.status, 409);
    assert.equal(await outboxCount(key), 1);
  });

  it('20 parallel requests with one new key -> one outbox row, one event_id', async () => {
    const key = `k-${randomUUID()}`;
    const results = await Promise.all(
      Array.from({ length: 20 }, () => post(key, { amount: 7, note: 'race' }))
    );
    assert.ok(results.every((r) => r.status === 202));
    assert.equal(new Set(results.map((r) => r.body.event_id)).size, 1);
    assert.equal(await outboxCount(key), 1);
  });

  it('key-order-different payload is a replay, not a conflict', async () => {
    const key = `k-${randomUUID()}`;
    const first = await post(key, { amount: 5, note: 'n' });
    const second = await request(app)
      .post('/api/events')
      .set('Idempotency-Key', key)
      .set('Content-Type', 'application/json')
      .send('{"note":"n","amount":5}');
    assert.equal(second.status, 202);
    assert.equal(second.body.event_id, first.body.event_id);
    assert.equal(second.headers['idempotency-replayed'], 'true');
  });

  it('GET /api/events/:id reports pending, then applied', async () => {
    const key = `k-${randomUUID()}`;
    const { body } = await post(key, { amount: 9 });
    const res = await request(app).get(`/api/events/${body.event_id}`);
    assert.equal(res.status, 200);
    assert.equal(res.body.status, 'pending');
    assert.equal(res.body.event_id, body.event_id);

    await pool.query("UPDATE outbox_events SET status='PUBLISHED', published_at=now() WHERE event_id=$1", [
      body.event_id,
    ]);
    assert.equal((await request(app).get(`/api/events/${body.event_id}`)).body.status, 'published');
    await pool.query('INSERT INTO business_state(event_id, amount) VALUES ($1, 9)', [body.event_id]);
    assert.equal((await request(app).get(`/api/events/${body.event_id}`)).body.status, 'applied');

    const missing = await request(app).get(`/api/events/${randomUUID()}`);
    assert.equal(missing.status, 404);
    assert.equal(missing.body.error, 'not_found');
  });

  it('/ready is 200 against a live database', async () => {
    assert.equal((await request(app).get('/ready')).status, 200);
  });

  it('redis stopped -> still 202 (fails open, quickly)', async () => {
    await redisContainer.stop();
    const key = `k-${randomUUID()}`;
    const started = Date.now();
    const first = await post(key, { amount: 1 });
    const second = await post(key, { amount: 1 });
    assert.equal(first.status, 202);
    assert.equal(second.status, 202);
    assert.equal(second.body.event_id, first.body.event_id);
    assert.equal(second.headers['idempotency-replayed'], 'true');
    assert.ok(Date.now() - started < 5000, 'requests must not hang on a dead redis');
    assert.equal(await outboxCount(key), 1);
  });
});
