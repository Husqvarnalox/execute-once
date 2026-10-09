package dev.executeonce.consumer;

import java.math.BigDecimal;
import java.util.UUID;

public record ParsedEvent(UUID eventId, BigDecimal amount, String note) {}
