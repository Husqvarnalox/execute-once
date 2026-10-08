package com.example.relay;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate txTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    private final RelayProperties props;

    private final RowMapper<OutboxEvent> mapper = new RowMapper<>() {
        @Override
        public OutboxEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new OutboxEvent(
                    (UUID) rs.getObject("event_id"),
                    rs.getString("idem_key"),
                    rs.getString("event_type"),
                    rs.getString("payload_json"),
                    rs.getString("status"),
                    rs.getInt("attempt"),
                    rs.getObject("next_attempt_at", OffsetDateTime.class)
            );
        }
    };

    @Scheduled(fixedDelayString = "${relay.poll-interval-ms:1000}")
    public void pollAndPublish() {
        List<OutboxEvent> batch = claimBatch();
        for (OutboxEvent event : batch) {
            if (!allowRateLimit()) {
                log.debug("rate limit hit, skipping publication for now");
                continue;
            }
            publishOnce(event);
        }
    }

    private List<OutboxEvent> claimBatch() {
        return txTemplate.execute(status -> {
            List<OutboxEvent> events = jdbcTemplate.query(
                    "SELECT event_id, idem_key, event_type, payload::text as payload_json, status, attempt, next_attempt_at " +
                            "FROM outbox_events WHERE status='PENDING' AND next_attempt_at <= now() " +
                            "FOR UPDATE SKIP LOCKED LIMIT ?",
                    mapper,
                    props.batchSize()
            );

            events.forEach(e -> jdbcTemplate.update(
                    "UPDATE outbox_events SET next_attempt_at = now() + interval '15 seconds' WHERE event_id = ?",
                    e.eventId()
            ));
            return events;
        });
    }

    private boolean allowRateLimit() {
        try {
            String key = "rl:relay:global:" + Instant.now().getEpochSecond();
            Long current = redisTemplate.opsForValue().increment(key, 1);
            if (current != null && current == 1) {
                redisTemplate.expire(key, Duration.ofSeconds(2));
            }
            return current == null || current <= props.rateLimitPerSecond();
        } catch (Exception ex) {
            log.warn("redis rate limit unavailable, allowing publish", ex);
            return true;
        }
    }

    private void publishOnce(OutboxEvent event) {
        try {
            kafkaTemplate.send(props.topic(), event.eventId().toString(), event.payloadJson()).get(10, TimeUnit.SECONDS);
            txTemplate.executeWithoutResult(status -> {
                jdbcTemplate.update("UPDATE outbox_events SET status='PUBLISHED', published_at=now() WHERE event_id=?",
                        event.eventId());
                jdbcTemplate.update("INSERT INTO relay_attempts(event_id, attempt, status) VALUES (?,?,?)",
                        event.eventId(), event.attempt(), "SUCCESS");
            });
            log.info("published event {}", event.eventId());
        } catch (Exception ex) {
            int nextAttempt = event.attempt() + 1;
            long backoffMs = props.baseBackoffMs() * (1L << Math.min(nextAttempt - 1, 6));
            long jitter = ThreadLocalRandom.current().nextLong(props.baseBackoffMs());
            Duration delay = Duration.ofMillis(backoffMs + jitter);
            txTemplate.executeWithoutResult(status -> {
                if (nextAttempt > props.maxAttempts()) {
                    jdbcTemplate.update("UPDATE outbox_events SET status='FAILED', attempt=? WHERE event_id=?",
                            nextAttempt, event.eventId());
                } else {
                    jdbcTemplate.update(
                            "UPDATE outbox_events SET attempt=?, next_attempt_at=now() + make_interval(secs => ?) WHERE event_id=?",
                            nextAttempt,
                            delay.toMillis() / 1000.0,
                            event.eventId());
                }
                jdbcTemplate.update("INSERT INTO relay_attempts(event_id, attempt, status, error) VALUES (?,?,?,?)",
                        event.eventId(), nextAttempt, "ERROR", ex.getMessage());
            });
            log.warn("failed to publish {} attempt {}", event.eventId(), nextAttempt, ex);
        }
    }
}
