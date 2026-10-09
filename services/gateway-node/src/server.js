import pino from 'pino';
import { loadConfig } from './config.js';
import { createApp } from './app.js';
import { createMetrics } from './metrics.js';
import { createPool, createRedis } from './clients.js';

let config;
try {
  config = loadConfig();
} catch (err) {
  console.error(`configuration error: ${err.message}`);
  process.exit(2);
}

const log = pino({ level: config.logLevel });
const pool = createPool(config, log);
const redis = createRedis(config, log);
const metrics = createMetrics();
const app = createApp({ pg: pool, redis, log, metrics, config });

const server = app.listen(config.port, () => {
  log.info({ port: config.port }, 'gateway listening');
});

let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info({ signal }, 'shutting down');
  const timer = setTimeout(() => {
    log.error('shutdown timed out, forcing exit');
    process.exit(1);
  }, config.shutdownTimeoutMs);
  timer.unref();

  // Stop accepting; resolves once in-flight requests finish.
  await new Promise((resolve) => {
    server.close(() => resolve());
    server.closeIdleConnections();
  });
  await pool.end().catch((err) => log.warn({ err }, 'pool end failed'));
  await redis.quit().catch(() => redis.disconnect());
  log.info('shutdown complete');
  process.exit(0);
}

for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => shutdown(sig));
