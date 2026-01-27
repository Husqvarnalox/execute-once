package com.example.relay;

import java.time.OffsetDateTime;
import java.util.UUID;

public record OutboxEvent(
        UUID eventId,
        String idemKey,
        String eventType,
        String payloadJson,
        String status,
        int attempt,
        OffsetDateTime nextAttemptAt
) {}
