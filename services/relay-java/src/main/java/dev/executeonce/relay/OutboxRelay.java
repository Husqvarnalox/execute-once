package dev.executeonce.relay;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxRelay {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
  static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);
  static final int MAX_BATCHES_PER_POLL = 100;

  private record InFlight(
      OutboxEvent event,
      CompletableFuture<SendResult<String, String>> future,
      Timer.Sample sample) {}

  private final OutboxRepository repository;
  private final KafkaTemplate<String, String> kafka;
  private final RateLimiter rateLimiter;
  private final BackoffPolicy backoff;
  private final RelayProperties props;
  private final Counter published;
  private final Counter failedRetryable;
  private final Counter failedTerminal;
  private final Timer publishTimer;
  private final AtomicLong backlog = new AtomicLong();

  public OutboxRelay(
      OutboxRepository repository,
      KafkaTemplate<String, String> kafka,
      RateLimiter rateLimiter,
      BackoffPolicy backoff,
      RelayProperties props,
      MeterRegistry registry) {
    this.repository = repository;
    this.kafka = kafka;
    this.rateLimiter = rateLimiter;
    this.backoff = backoff;
    this.props = props;
    this.published = registry.counter("outbox_relay_published_total");
    this.failedRetryable = registry.counter("outbox_relay_failed_total", "terminal", "false");
    this.failedTerminal = registry.counter("outbox_relay_failed_total", "terminal", "true");
    this.publishTimer = registry.timer("outbox_relay_publish_seconds");
    registry.gauge("outbox_relay_backlog", backlog);
  }

  /**
   * One scheduled tick drains the outbox: batches are claimed and published until a batch comes
   * back short (nothing left that is due), the rate limiter says stop, or the per-tick cap is hit.
   * Without the loop, throughput would be capped at batch-size / poll-interval.
   */
  @Scheduled(fixedDelayString = "${relay.poll-interval:1s}")
  public void pollAndPublish() {
    try {
      backlog.set(repository.countBacklog());
    } catch (Exception ex) {
      log.warn("could not refresh backlog gauge: {}", ex.toString());
    }

    for (int i = 0; i < MAX_BATCHES_PER_POLL; i++) {
      List<OutboxEvent> batch = repository.claimBatch(props.batchSize(), props.lease());
      if (batch.isEmpty()) {
        return;
      }
      boolean throttled = publishBatch(batch);
      if (throttled || batch.size() < props.batchSize()) {
        return;
      }
    }
    log.debug("per-tick batch cap reached, remaining rows wait for the next poll");
  }

  /** Publishes one claimed batch. Returns true when the rate limiter stopped it early. */
  private boolean publishBatch(List<OutboxEvent> batch) {
    List<InFlight> inFlight = new ArrayList<>(batch.size());
    List<OutboxEvent> throttled = new ArrayList<>();
    for (OutboxEvent event : batch) {
      // Once the limiter denies, hand the rest of the batch back immediately rather than
      // letting it sit invisible until the lease expires.
      if (!throttled.isEmpty() || !rateLimiter.tryAcquire()) {
        throttled.add(event);
        continue;
      }
      Timer.Sample sample = Timer.start();
      CompletableFuture<SendResult<String, String>> future;
      try {
        future = kafka.send(props.topic(), event.eventId().toString(), event.payloadJson());
      } catch (Exception ex) {
        future = CompletableFuture.failedFuture(ex);
      }
      inFlight.add(new InFlight(event, future, sample));
    }
    if (!throttled.isEmpty()) {
      log.debug("rate limit hit, releasing {} claimed events", throttled.size());
      repository.releaseLease(throttled);
    }

    for (InFlight f : inFlight) {
      try {
        f.future().get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        f.sample().stop(publishTimer);
        repository.markPublished(f.event());
        published.increment();
        log.debug("published event {}", f.event().eventId());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        f.sample().stop(publishTimer);
        handleFailure(f.event(), ie);
        return true;
      } catch (Exception ex) {
        f.sample().stop(publishTimer);
        handleFailure(f.event(), ex);
      }
    }
    return !throttled.isEmpty();
  }

  private void handleFailure(OutboxEvent event, Exception ex) {
    int nextAttempt = event.attempt() + 1;
    Throwable cause =
        ex instanceof java.util.concurrent.ExecutionException && ex.getCause() != null
            ? ex.getCause()
            : ex;
    String error = cause.getMessage() != null ? cause.getMessage() : cause.toString();
    if (nextAttempt > props.maxAttempts()) {
      repository.markFailed(event, error);
      failedTerminal.increment();
      log.error("event {} permanently failed after {} attempts", event.eventId(), nextAttempt, ex);
    } else {
      repository.scheduleRetry(event, backoff.delayFor(nextAttempt), error);
      failedRetryable.increment();
      log.warn("failed to publish {} attempt {}: {}", event.eventId(), nextAttempt, error);
    }
  }
}
