package dev.executeonce.relay;

public interface RateLimiter {
  /** Returns true if one more unit of work may proceed within the current window. */
  boolean tryAcquire();
}
