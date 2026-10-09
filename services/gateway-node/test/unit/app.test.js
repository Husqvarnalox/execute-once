import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';
import pino from 'pino';
import { createApp } from '../../src/app.js';
import { createMetrics } from '../../src/metrics.js';
import { requestHash } from '../../src/canonical.js';

const config = { bodyLimit: '16kb', idemTtlSeconds: 60, eventType: 'event.created' };
const EVENT_ID = '11111111-1111-4111-8111-111111111111';

function fakeRedis(store = new Map()) {
  return {
    store,
    get: async (k) => store.get(k) ?? null,
    set: async (k, v) => {
      store.set(k, v);
      return 'OK';
    },
  };
}

/** Minimal pg stub: records queries, returns a COMPLETED-less fresh row. */
function fakePg(onQuery) {
  const calls = [];
  const client = {
    query: async (sql, params) => {
      calls.push(String(sql).trim().split(/\s+/).slice(0, 2).join(' '));
      if (onQuery) return onQuery(sql, params);
      if (/FOR UPDATE/.test(sql))
        return { rows: [{ request_hash: params.hashRef.h, status: 'IN_PROGRESS' }] };
      return { rows: [] };
    },
    release: () => {},
  };
  return { calls, connect: async () => client, query: async () => ({ rows: [{}] }) };
}

function build({ pg, redis } = {}) {
  const log = pino({ level: 'silent' });
  const metrics = createMetrics();
  return createApp({ pg: pg ?? fakePg(), redis: redis ?? fakeRedis(), log, metrics, config });
}

// pg stub that echoes whatever hash was inserted so the FOR UPDATE row always matches
function matchingPg() {
  let inserted;
  return fakePg(async (sql, params) => {
    if (/^INSERT INTO idempotency_keys/.test(sql)) inserted = params[1];
    if (/FOR UPDATE/.test(sql)) return { rows: [{ request_hash: inserted, status: 'IN_PROGRESS' }] };
    return { rows: [] };
  });
}

describe('POST /api/events (unit, fakes)', () => {
  let app;
  beforeEach(() => {
    app = build();
  });

  it('400 missing_idempotency_key', async () => {
    const res = await request(app).post('/api/events').send({ amount: 1 });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'missing_idempotency_key');
    assert.ok(res.headers['x-request-id']);
  });

  it('400 invalid_idempotency_key', async () => {
    const res = await request(app)
      .post('/api/events')
      .set('Idempotency-Key', 'k'.repeat(200))
      .send({ amount: 1 });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'invalid_idempotency_key');
  });

  it('400 validation_error for invalid body with details', async () => {
    const res = await request(app).post('/api/events').set('Idempotency-Key', 'k1').send({ amount: -5 });
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'validation_error');
    assert.ok(Array.isArray(res.body.details));
  });

  it('400 validation_error for malformed JSON', async () => {
    const res = await request(app)
      .post('/api/events')
      .set('Idempotency-Key', 'k1')
      .set('Content-Type', 'application/json')
      .send('{"amount":');
    assert.equal(res.status, 400);
    assert.equal(res.body.error, 'validation_error');
  });

  it('echoes incoming X-Request-Id', async () => {
    const res = await request(app).get('/health').set('X-Request-Id', 'req-42');
    assert.equal(res.headers['x-request-id'], 'req-42');
    assert.deepEqual(res.body, { status: 'ok' });
  });

  it('redis cache hit replays with Idempotency-Replayed', async () => {
    const body = { amount: 3 };
    const redis = fakeRedis();
    redis.store.set(
      'idem:k1',
      JSON.stringify({
        hash: requestHash(body),
        status: 202,
        body: { event_id: EVENT_ID, status: 'accepted' },
      })
    );
    const pg = fakePg();
    const res = await request(build({ pg, redis }))
      .post('/api/events')
      .set('Idempotency-Key', 'k1')
      .send(body);
    assert.equal(res.status, 202);
    assert.equal(res.headers['idempotency-replayed'], 'true');
    assert.equal(res.body.event_id, EVENT_ID);
    assert.equal(pg.calls.length, 0);
  });

  it('redis cache hit with different hash -> 409', async () => {
    const redis = fakeRedis();
    redis.store.set(
      'idem:k1',
      JSON.stringify({ hash: requestHash({ amount: 1 }), status: 202, body: { event_id: EVENT_ID } })
    );
    const res = await request(build({ redis }))
      .post('/api/events')
      .set('Idempotency-Key', 'k1')
      .send({ amount: 2 });
    assert.equal(res.status, 409);
    assert.equal(res.body.error, 'idempotency_key_conflict');
  });

  it('redis throwing falls through to postgres path', async () => {
    const redis = {
      get: async () => {
        throw new Error('redis down');
      },
      set: async () => {
        throw new Error('redis down');
      },
    };
    const pg = matchingPg();
    const res = await request(build({ pg, redis }))
      .post('/api/events')
      .set('Idempotency-Key', 'k1')
      .send({ amount: 2 });
    assert.equal(res.status, 202);
    assert.equal(res.body.status, 'accepted');
    assert.equal(res.headers['idempotency-replayed'], undefined);
    assert.ok(pg.calls.includes('COMMIT'));
  });

  it('500 internal_error contract without leaking details', async () => {
    const pg = fakePg(async () => {
      throw new Error('secret db detail');
    });
    const res = await request(build({ pg }))
      .post('/api/events')
      .set('Idempotency-Key', 'k1')
      .send({ amount: 2 });
    assert.equal(res.status, 500);
    assert.equal(res.body.error, 'internal_error');
    assert.ok(!JSON.stringify(res.body).includes('secret'));
  });

  it('404 not_found for unknown route and malformed event id', async () => {
    const a = await request(app).get('/nope');
    assert.equal(a.status, 404);
    assert.equal(a.body.error, 'not_found');
    const b = await request(app).get('/api/events/not-a-uuid');
    assert.equal(b.status, 404);
  });

  it('/ready returns 503 when postgres fails', async () => {
    const pg = { query: async () => Promise.reject(new Error('down')), connect: async () => {} };
    const res = await request(build({ pg })).get('/ready');
    assert.equal(res.status, 503);
  });

  it('/metrics exposes custom metrics', async () => {
    const res = await request(app).get('/metrics');
    assert.equal(res.status, 200);
    assert.match(res.text, /idempotency_conflicts_total/);
    assert.match(res.text, /events_accepted_total/);
  });
});
