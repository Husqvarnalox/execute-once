package com.example.relay;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "relay")
public record RelayProperties(
        long pollIntervalMs,
        int batchSize,
        int maxAttempts,
        long baseBackoffMs,
        int rateLimitPerSecond,
        String topic
) {}
