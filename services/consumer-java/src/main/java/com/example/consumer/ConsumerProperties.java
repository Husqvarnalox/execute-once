package com.example.consumer;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "consumer")
public record ConsumerProperties(
        String topic,
        int rateLimitPerSecond,
        int maxParallelism
) {}
