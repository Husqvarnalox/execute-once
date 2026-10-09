package dev.executeonce.consumer;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Fixed one-second window counter in Redis. Fails open on Redis errors. */
public class RedisFixedWindowRateLimiter implements RateLimiter {

  private static final Logger log = LoggerFactory.getLogger(RedisFixedWindowRateLimiter.class);
  private static final long WARN_INTERVAL_MS = 60_000;

  private final StringRedisTemplate redis;
  private final String keyPrefix;
  private final int limitPerSecond;
  private final Clock clock;
  private final AtomicLong lastWarnMs = new AtomicLong(Long.MIN_VALUE);

  public RedisFixedWindowRateLimiter(
      StringRedisTemplate redis, String keyPrefix, int limitPerSecond, Clock clock) {
    this.redis = redis;
    this.keyPrefix = keyPrefix;
    this.limitPerSecond = limitPerSecond;
    this.clock = clock;
  }

  @Override
  public boolean tryAcquire() {
    try {
      String key = keyPrefix + clock.instant().getEpochSecond();
      Long current = redis.opsForValue().increment(key);
      if (current != null && current == 1) {
        redis.expire(key, Duration.ofSeconds(2));
      }
      return current == null || current <= limitPerSecond;
    } catch (Exception ex) {
      warnThrottled(ex);
      return true;
    }
  }

  private void warnThrottled(Exception ex) {
    long now = clock.millis();
    long last = lastWarnMs.get();
    if ((last == Long.MIN_VALUE || now - last >= WARN_INTERVAL_MS)
        && lastWarnMs.compareAndSet(last, now)) {
      log.warn("redis rate limiter unavailable, failing open: {}", ex.toString());
    }
  }
}
