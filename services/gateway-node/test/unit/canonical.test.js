import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { canonicalize, requestHash } from '../../src/canonical.js';

describe('canonicalize', () => {
  it('is independent of key order', () => {
    assert.equal(canonicalize({ a: 1, b: 2 }), canonicalize({ b: 2, a: 1 }));
    assert.equal(requestHash({ a: 1, b: 2 }), requestHash({ b: 2, a: 1 }));
  });

  it('sorts nested objects, including inside arrays', () => {
    const x = { z: { b: 1, a: [{ y: 1, x: 2 }] }, a: 0 };
    const y = { a: 0, z: { a: [{ x: 2, y: 1 }], b: 1 } };
    assert.equal(canonicalize(x), canonicalize(y));
    assert.equal(canonicalize(x), '{"a":0,"z":{"a":[{"x":2,"y":1}],"b":1}}');
  });

  it('preserves array order', () => {
    assert.notEqual(canonicalize([1, 2]), canonicalize([2, 1]));
    assert.notEqual(requestHash({ a: [1, 2] }), requestHash({ a: [2, 1] }));
  });

  it('handles unicode and escapes deterministically', () => {
    assert.equal(canonicalize({ é: 'ñ', a: '☃"\n' }), '{"a":"☃\\"\\n","é":"ñ"}');
    assert.equal(requestHash({ note: 'héllo ☃' }), requestHash({ note: 'héllo ☃' }));
  });

  it('distinguishes different values and types', () => {
    assert.notEqual(requestHash({ a: 1 }), requestHash({ a: '1' }));
    assert.notEqual(requestHash({ a: null }), requestHash({}));
  });

  it('produces a sha256 hex digest', () => {
    assert.match(requestHash({}), /^[0-9a-f]{64}$/);
  });
});
