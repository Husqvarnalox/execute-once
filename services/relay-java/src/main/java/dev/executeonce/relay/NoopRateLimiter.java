package dev.executeonce.relay;

public class NoopRateLimiter implements RateLimiter {
  @Override
  public boolean tryAcquire() {
    return true;
  }
}
