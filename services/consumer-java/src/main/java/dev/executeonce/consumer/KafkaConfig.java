package dev.executeonce.consumer;

import java.time.Clock;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Consumer factory, ack mode and group id come from Boot auto-configuration (see application.yml);
 * a CommonErrorHandler bean is picked up by the auto-configured listener container factory.
 */
@Configuration
public class KafkaConfig {

  @Bean
  public Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  public NewTopic deadLetterTopic(ConsumerProperties props) {
    return TopicBuilder.name(props.deadLetterTopic())
        .partitions(props.concurrency())
        .replicas(1)
        .build();
  }

  @Bean
  public RateLimiter rateLimiter(ConsumerProperties props, StringRedisTemplate redis, Clock clock) {
    if (props.rateLimitPerSecond() <= 0) {
      return new NoopRateLimiter();
    }
    return new RedisFixedWindowRateLimiter(
        redis, "rl:consumer:global:", props.rateLimitPerSecond(), clock);
  }

  @Bean
  public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
      KafkaTemplate<String, String> template, ConsumerProperties props) {
    return new DeadLetterPublishingRecoverer(
        template, (record, ex) -> new TopicPartition(props.deadLetterTopic(), -1));
  }

  @Bean
  public DefaultErrorHandler kafkaErrorHandler(
      DeadLetterPublishingRecoverer dlpr, DeadLetterAuditor auditor, ConsumerProperties props) {
    ConsumerRecordRecoverer recoverer =
        (record, ex) -> {
          auditor.recordDeadLettered(record, ex);
          dlpr.accept(record, ex);
        };
    ExponentialBackOffWithMaxRetries backOff =
        new ExponentialBackOffWithMaxRetries(props.maxRetries());
    backOff.setInitialInterval(props.retryBackoff().toMillis());
    backOff.setMultiplier(2.0);
    backOff.setMaxInterval(Math.max(props.retryBackoff().toMillis(), 30_000L));
    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
    handler.addNotRetryableExceptions(PoisonRecordException.class);
    return handler;
  }
}
