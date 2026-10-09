package dev.executeonce.relay;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Random;
import org.junit.jupiter.api.Test;

class BackoffPolicyTest {

  private final BackoffPolicy policy =
      new BackoffPolicy(Duration.ofMillis(500), Duration.ofSeconds(60), new Random(42));

  @Test
  void ceilingGrowsExponentially() {
    assertThat(policy.ceilingFor(1)).isEqualTo(Duration.ofMillis(500));
    assertThat(policy.ceilingFor(2)).isEqualTo(Duration.ofMillis(1000));
    assertThat(policy.ceilingFor(3)).isEqualTo(Duration.ofMillis(2000));
    assertThat(policy.ceilingFor(6)).isEqualTo(Duration.ofMillis(16000));
  }

  @Test
  void neverExceedsMaxBackoff() {
    for (int attempt = 1; attempt <= 200; attempt++) {
      assertThat(policy.ceilingFor(attempt)).isLessThanOrEqualTo(Duration.ofSeconds(60));
      assertThat(policy.delayFor(attempt)).isLessThanOrEqualTo(Duration.ofSeconds(60));
    }
    assertThat(policy.ceilingFor(200)).isEqualTo(Duration.ofSeconds(60));
  }

  @Test
  void jitterStaysWithinZeroAndCeiling() {
    for (int attempt = 1; attempt <= 12; attempt++) {
      Duration ceiling = policy.ceilingFor(attempt);
      for (int i = 0; i < 200; i++) {
        assertThat(policy.delayFor(attempt)).isBetween(Duration.ZERO, ceiling);
      }
    }
  }

  @Test
  void deterministicWithSameSeed() {
    BackoffPolicy a =
        new BackoffPolicy(Duration.ofMillis(500), Duration.ofSeconds(60), new Random(7));
    BackoffPolicy b =
        new BackoffPolicy(Duration.ofMillis(500), Duration.ofSeconds(60), new Random(7));
    for (int attempt = 1; attempt <= 10; attempt++) {
      assertThat(a.delayFor(attempt)).isEqualTo(b.delayFor(attempt));
    }
  }
}
