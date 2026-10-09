import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { validateEventBody, validateIdempotencyKey } from '../../src/validation.js';

describe('validateIdempotencyKey', () => {
  it('accepts printable ASCII of 1-128 chars', () => {
    assert.equal(validateIdempotencyKey('abc-123_XYZ'), null);
    assert.equal(validateIdempotencyKey('a'.repeat(128)), null);
  });

  it('rejects missing/empty', () => {
    assert.equal(validateIdempotencyKey(undefined).error, 'missing_idempotency_key');
    assert.equal(validateIdempotencyKey('').error, 'missing_idempotency_key');
  });

  it('rejects too long and non-printable/non-ASCII', () => {
    assert.equal(validateIdempotencyKey('a'.repeat(129)).error, 'invalid_idempotency_key');
    assert.equal(validateIdempotencyKey('bad\u0001key').error, 'invalid_idempotency_key');
    assert.equal(validateIdempotencyKey('clé').error, 'invalid_idempotency_key');
  });
});

describe('validateEventBody', () => {
  it('accepts valid bodies', () => {
    assert.deepEqual(validateEventBody({ amount: 0 }), []);
    assert.deepEqual(validateEventBody({ amount: 12.5, note: 'hi' }), []);
    assert.deepEqual(validateEventBody({ amount: 1, note: 'x'.repeat(256) }), []);
  });

  it('rejects non-objects', () => {
    for (const b of [undefined, null, [], 'x', 5]) {
      assert.equal(validateEventBody(b)[0].field, '$');
    }
  });

  it('requires amount', () => {
    assert.equal(validateEventBody({})[0].field, 'amount');
  });

  it('rejects bad amounts', () => {
    for (const amount of [-1, '5', null, NaN, Infinity, true, 1.005, 10_000_000_000]) {
      assert.equal(validateEventBody({ amount })[0].field, 'amount', String(amount));
    }
  });

  it('rejects bad notes', () => {
    assert.equal(validateEventBody({ amount: 1, note: 5 })[0].field, 'note');
    assert.equal(validateEventBody({ amount: 1, note: 'x'.repeat(257) })[0].field, 'note');
  });

  it('rejects unknown fields', () => {
    const d = validateEventBody({ amount: 1, extra: true });
    assert.deepEqual(d, [{ field: 'extra', message: 'unknown field' }]);
  });
});
