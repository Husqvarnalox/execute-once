package dev.executeonce.consumer;

public class NoopRateLimiter implements RateLimiter {
  @Override
  public boolean tryAcquire() {
    return true;
  }
}
