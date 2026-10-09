import { createHash } from 'node:crypto';

/** Deterministic JSON: object keys sorted recursively, array order preserved. */
export function canonicalize(value) {
  if (Array.isArray(value)) {
    return `[${value.map((v) => canonicalize(v === undefined ? null : v)).join(',')}]`;
  }
  if (value !== null && typeof value === 'object') {
    const parts = Object.keys(value)
      .filter((k) => value[k] !== undefined)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canonicalize(value[k])}`);
    return `{${parts.join(',')}}`;
  }
  return JSON.stringify(value);
}

export function requestHash(body) {
  return createHash('sha256').update(canonicalize(body)).digest('hex');
}
