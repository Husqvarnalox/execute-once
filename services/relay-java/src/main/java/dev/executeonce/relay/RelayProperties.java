package dev.executeonce.relay;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "relay")
public record RelayProperties(
    @DefaultValue("1s") Duration pollInterval,
    @DefaultValue("200") int batchSize,
    @DefaultValue("10") int maxAttempts,
    @DefaultValue("500ms") Duration baseBackoff,
    @DefaultValue("60s") Duration maxBackoff,
    @DefaultValue("15s") Duration lease,
    @DefaultValue("1000") int rateLimitPerSecond,
    @DefaultValue("events") String topic,
    @DefaultValue("3") int topicPartitions,
    @DefaultValue("168h") Duration retention,
    @DefaultValue("10m") Duration cleanupInterval,
    @DefaultValue("1000") int cleanupBatchSize) {}
