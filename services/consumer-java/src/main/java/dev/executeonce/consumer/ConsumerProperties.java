package dev.executeonce.consumer;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "consumer")
public record ConsumerProperties(
    @DefaultValue("events") String topic,
    @DefaultValue("events.dlq") String deadLetterTopic,
    @DefaultValue("inbox-consumer") String groupId,
    @DefaultValue("3") int concurrency,
    @DefaultValue("1000") int rateLimitPerSecond,
    @DefaultValue("5") int maxRetries,
    @DefaultValue("500ms") Duration retryBackoff) {}
