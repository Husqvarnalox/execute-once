const LOG_LEVELS = ['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'];

function intVar(env, name, def, { min = 0, max = Number.MAX_SAFE_INTEGER } = {}) {
  const raw = env[name];
  if (raw === undefined || raw === '') return def;
  if (!/^-?\d+$/.test(raw)) throw new Error(`Invalid ${name}: expected an integer, got "${raw}"`);
  const n = Number(raw);
  if (n < min || n > max) throw new Error(`Invalid ${name}: ${n} not in [${min}, ${max}]`);
  return n;
}

function strVar(env, name, def) {
  const raw = env[name];
  return raw === undefined || raw === '' ? def : raw;
}

export function loadConfig(env = process.env) {
  const logLevel = strVar(env, 'LOG_LEVEL', 'info');
  if (!LOG_LEVELS.includes(logLevel)) {
    throw new Error(`Invalid LOG_LEVEL: "${logLevel}" (one of ${LOG_LEVELS.join(', ')})`);
  }
  const redisUrl = strVar(env, 'REDIS_URL', 'redis://127.0.0.1:6379');
  try {
    new URL(redisUrl);
  } catch {
    throw new Error('Invalid REDIS_URL: not a valid URL');
  }
  return {
    port: intVar(env, 'PORT', 3000, { min: 0, max: 65535 }),
    pg: {
      host: strVar(env, 'PGHOST', '127.0.0.1'),
      port: intVar(env, 'PGPORT', 5432, { min: 1, max: 65535 }),
      user: strVar(env, 'PGUSER', 'app'),
      password: strVar(env, 'PGPASSWORD', 'app'),
      database: strVar(env, 'PGDATABASE', 'app'),
      max: intVar(env, 'PGPOOL_MAX', 20, { min: 1, max: 1000 }),
    },
    redisUrl,
    idemTtlSeconds: intVar(env, 'IDEM_TTL_SECONDS', 3600, { min: 1 }),
    logLevel,
    bodyLimit: strVar(env, 'BODY_LIMIT', '16kb'),
    shutdownTimeoutMs: intVar(env, 'SHUTDOWN_TIMEOUT_MS', 10000, { min: 1 }),
    eventType: strVar(env, 'EVENT_TYPE', 'event.created'),
  };
}
