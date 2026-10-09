import pg from 'pg';
import Redis from 'ioredis';

export function createPool(config, log) {
  const pool = new pg.Pool({ ...config.pg });
  // Idle-client errors (e.g. server restart) must not crash the process.
  pool.on('error', (err) => log.error({ err }, 'idle postgres client error'));
  return pool;
}

/** Fail-fast Redis: commands error immediately when disconnected, so the cache fails open. */
export function createRedis(config, log) {
  const redis = new Redis(config.redisUrl, {
    enableOfflineQueue: false,
    maxRetriesPerRequest: 1,
    connectTimeout: 2000,
    lazyConnect: false,
  });
  redis.on('error', (err) => log.warn({ err: err.message }, 'redis error'));
  return redis;
}
