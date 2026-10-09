package dev.executeonce.relay;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/** Exponential backoff with full jitter, capped at {@code maxBackoff}. */
public class BackoffPolicy {

  private final Duration base;
  private final Duration max;
  private final Random random;

  public BackoffPolicy(Duration base, Duration max) {
    this(base, max, ThreadLocalRandom.current());
  }

  public BackoffPolicy(Duration base, Duration max, Random random) {
    this.base = base;
    this.max = max;
    this.random = random;
  }

  /** Upper bound (before jitter) for the given 1-based attempt number. */
  public Duration ceilingFor(int attempt) {
    int shift = Math.min(Math.max(attempt - 1, 0), 30);
    long baseMs = base.toMillis();
    long ceilingMs = baseMs > Long.MAX_VALUE >> shift ? Long.MAX_VALUE : baseMs << shift;
    return Duration.ofMillis(Math.min(ceilingMs, max.toMillis()));
  }

  /** Random delay in [0, ceiling] (full jitter). */
  public Duration delayFor(int attempt) {
    long ceilingMs = ceilingFor(attempt).toMillis();
    if (ceilingMs <= 0) {
      return Duration.ZERO;
    }
    long bound = ceilingMs + 1;
    long jittered = (random.nextLong() & Long.MAX_VALUE) % bound;
    return Duration.ofMillis(jittered);
  }
}
