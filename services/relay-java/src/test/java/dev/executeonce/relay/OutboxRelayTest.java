package dev.executeonce.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@SuppressWarnings("unchecked")
class OutboxRelayTest {

  private OutboxRepository repo;
  private KafkaTemplate<String, String> kafka;
  private RateLimiter limiter;
  private SimpleMeterRegistry registry;
  private OutboxRelay relay;

  private static final RelayProperties PROPS =
      new RelayProperties(
          Duration.ofSeconds(1),
          50,
          3,
          Duration.ofMillis(500),
          Duration.ofSeconds(60),
          Duration.ofSeconds(15),
          0,
          "events",
          3,
          Duration.ofHours(168),
          Duration.ofMinutes(10),
          1000);

  @BeforeEach
  void setUp() {
    repo = mock(OutboxRepository.class);
    kafka = mock(KafkaTemplate.class);
    limiter = mock(RateLimiter.class);
    registry = new SimpleMeterRegistry();
    when(limiter.tryAcquire()).thenReturn(true);
    BackoffPolicy backoff =
        new BackoffPolicy(PROPS.baseBackoff(), PROPS.maxBackoff(), new Random(1));
    relay = new OutboxRelay(repo, kafka, limiter, backoff, PROPS, registry);
  }

  private OutboxEvent event(int attempt) {
    return new OutboxEvent(
        UUID.randomUUID(), "k", "t", "{\"amount\":1}", "PENDING", attempt, OffsetDateTime.now());
  }

  private void claim(OutboxEvent... events) {
    when(repo.claimBatch(anyInt(), any())).thenReturn(List.of(events));
  }

  @Test
  void successMarksPublished() {
    OutboxEvent e = event(0);
    claim(e);
    when(kafka.send(eq("events"), eq(e.eventId().toString()), anyString()))
        .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

    relay.pollAndPublish();

    verify(repo).markPublished(e);
    assertThat(registry.counter("outbox_relay_published_total").count()).isEqualTo(1.0);
  }

  @Test
  void failureSchedulesRetry() {
    OutboxEvent e = event(1);
    claim(e);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

    relay.pollAndPublish();

    verify(repo).scheduleRetry(eq(e), any(Duration.class), eq("broker down"));
    verify(repo, never()).markFailed(any(), any());
    assertThat(registry.counter("outbox_relay_failed_total", "terminal", "false").count())
        .isEqualTo(1.0);
  }

  @Test
  void failureBeyondMaxAttemptsMarksFailed() {
    OutboxEvent e = event(3); // next attempt = 4 > maxAttempts(3)
    claim(e);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));

    relay.pollAndPublish();

    verify(repo).markFailed(e, "boom");
    verify(repo, never()).scheduleRetry(any(), any(), any());
    assertThat(registry.counter("outbox_relay_failed_total", "terminal", "true").count())
        .isEqualTo(1.0);
  }

  @Test
  void rateLimitDenialReleasesLeaseAndStops() {
    OutboxEvent e = event(0);
    claim(e);
    when(limiter.tryAcquire()).thenReturn(false);

    relay.pollAndPublish();

    verify(repo).countBacklog();
    verify(repo).claimBatch(anyInt(), any());
    verify(repo).releaseLease(List.of(e));
    verifyNoMoreInteractions(repo);
    org.mockito.Mockito.verifyNoInteractions(kafka);
  }

  @Test
  void drainsUntilBatchComesBackShort() {
    OutboxEvent[] full = new OutboxEvent[PROPS.batchSize()];
    for (int i = 0; i < full.length; i++) {
      full[i] = event(0);
    }
    OutboxEvent last = event(0);
    when(repo.claimBatch(anyInt(), any()))
        .thenReturn(List.of(full))
        .thenReturn(List.of(last))
        .thenReturn(List.of());
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

    relay.pollAndPublish();

    verify(repo, org.mockito.Mockito.times(2)).claimBatch(anyInt(), any());
    verify(repo, org.mockito.Mockito.times(full.length + 1)).markPublished(any());
  }
}
