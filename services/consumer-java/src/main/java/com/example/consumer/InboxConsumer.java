package com.example.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class InboxConsumer {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate txTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ConsumerProperties props;
    private final ObjectMapper mapper = new ObjectMapper();

    @KafkaListener(topics = "${consumer.topic:events}", containerFactory = "kafkaListenerContainerFactory")
    public void handle(ConsumerRecord<String, String> record, Acknowledgment ack) {
        if (!allowRateLimit()) {
            ack.nack(Duration.ofMillis(1000));
            return;
        }

        try {
            processRecord(record);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("processing failed for offset {}", record.offset(), ex);
            throw ex;
        }
    }

    private boolean allowRateLimit() {
        try {
            String key = "rl:consumer:global:" + Instant.now().getEpochSecond();
            Long current = redisTemplate.opsForValue().increment(key, 1);
            if (current != null && current == 1) {
                redisTemplate.expire(key, Duration.ofSeconds(2));
            }
            return current == null || current <= props.rateLimitPerSecond();
        } catch (Exception ex) {
            log.warn("redis rate limit unavailable, allowing processing", ex);
            return true;
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        UUID eventId = UUID.fromString(record.key());
        JsonNode payload;
        try {
            payload = mapper.readTree(record.value());
        } catch (Exception e) {
            payload = mapper.createObjectNode();
        }
        double amount = payload.path("amount").asDouble(0.0);
        String note = payload.path("note").asText(null);

        txTemplate.executeWithoutResult(status -> {
            try {
                jdbcTemplate.update("INSERT INTO inbox_events(event_id) VALUES (?)", eventId);
            } catch (DuplicateKeyException dup) {
                jdbcTemplate.update("INSERT INTO consumer_attempts(event_id, status, error) VALUES (?,?,?)",
                        eventId, "DUPLICATE", null);
                return;
            }

            jdbcTemplate.update("INSERT INTO business_state(event_id, amount, note) VALUES (?,?,?)",
                    eventId, amount, note);
            jdbcTemplate.update("INSERT INTO consumer_attempts(event_id, status) VALUES (?,?)",
                    eventId, "APPLIED");
        });
    }
}
