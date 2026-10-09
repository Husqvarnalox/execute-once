const KEY_RE = /^[\x20-\x7e]{1,128}$/;
const ALLOWED_FIELDS = new Set(['amount', 'note']);
const NOTE_MAX = 256;
// business_state.amount is numeric(12,2): ten integer digits, two decimals.
const AMOUNT_MAX = 9_999_999_999.99;
const hasAtMostTwoDecimals = (n) => Math.round(n * 100) / 100 === n;

/** Returns null when valid, otherwise { error, message }. */
export function validateIdempotencyKey(value) {
  if (value === undefined || value === null || value === '') {
    return { error: 'missing_idempotency_key', message: 'Idempotency-Key header is required' };
  }
  if (typeof value !== 'string' || !KEY_RE.test(value)) {
    return {
      error: 'invalid_idempotency_key',
      message: 'Idempotency-Key must be 1-128 printable ASCII characters',
    };
  }
  return null;
}

/** Returns an array of { field, message } problems; empty when valid. */
export function validateEventBody(body) {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    return [{ field: '$', message: 'body must be a JSON object' }];
  }
  const details = [];
  if (!('amount' in body)) {
    details.push({ field: 'amount', message: 'amount is required' });
  } else if (typeof body.amount !== 'number' || !Number.isFinite(body.amount) || body.amount < 0) {
    details.push({ field: 'amount', message: 'amount must be a finite number >= 0' });
  } else if (body.amount > AMOUNT_MAX || !hasAtMostTwoDecimals(body.amount)) {
    details.push({ field: 'amount', message: `amount must be <= ${AMOUNT_MAX} with at most two decimals` });
  }
  if ('note' in body && (typeof body.note !== 'string' || body.note.length > NOTE_MAX)) {
    details.push({ field: 'note', message: `note must be a string of at most ${NOTE_MAX} characters` });
  }
  for (const key of Object.keys(body)) {
    if (!ALLOWED_FIELDS.has(key)) details.push({ field: key, message: 'unknown field' });
  }
  return details;
}
