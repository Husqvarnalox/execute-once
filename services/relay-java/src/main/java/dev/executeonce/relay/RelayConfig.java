package dev.executeonce.relay;

import java.time.Clock;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class RelayConfig {

  @Bean
  public Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  public BackoffPolicy backoffPolicy(RelayProperties props) {
    return new BackoffPolicy(props.baseBackoff(), props.maxBackoff());
  }

  @Bean
  public RateLimiter rateLimiter(RelayProperties props, StringRedisTemplate redis, Clock clock) {
    if (props.rateLimitPerSecond() <= 0) {
      return new NoopRateLimiter();
    }
    return new RedisFixedWindowRateLimiter(
        redis, "rl:relay:global:", props.rateLimitPerSecond(), clock);
  }

  @Bean
  public NewTopic outboxTopic(RelayProperties props) {
    return TopicBuilder.name(props.topic()).partitions(props.topicPartitions()).replicas(1).build();
  }
}
