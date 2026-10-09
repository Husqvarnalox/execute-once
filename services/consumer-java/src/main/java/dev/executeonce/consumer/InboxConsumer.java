package dev.executeonce.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class InboxConsumer {

  private static final Logger log = LoggerFactory.getLogger(InboxConsumer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_INTEGER_DIGITS = 10; // numeric(12,2)

  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final RateLimiter rateLimiter;
  private final Counter applied;
  private final Counter duplicate;
  private final Timer processTimer;

  public InboxConsumer(
      JdbcTemplate jdbc, TransactionTemplate tx, RateLimiter rateLimiter, MeterRegistry registry) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.rateLimiter = rateLimiter;
    this.applied = registry.counter("inbox_applied_total");
    this.duplicate = registry.counter("inbox_duplicate_total");
    this.processTimer = registry.timer("inbox_process_seconds");
  }

  @KafkaListener(topics = "${consumer.topic:events}", concurrency = "${consumer.concurrency:3}")
  public void handle(ConsumerRecord<String, String> record, Acknowledgment ack) {
    if (!rateLimiter.tryAcquire()) {
      ack.nack(Duration.ofSeconds(1));
      return;
    }
    Timer.Sample sample = Timer.start();
    try {
      ParsedEvent event = parse(record.key(), record.value());
      boolean wasApplied = apply(event);
      // acknowledge only after the transaction has committed
      ack.acknowledge();
      (wasApplied ? applied : duplicate).increment();
      log.debug("{} event {}", wasApplied ? "applied" : "duplicate", event.eventId());
    } finally {
      sample.stop(processTimer);
    }
  }

  /** Inserts inbox + business state atomically. Returns false when the event was a duplicate. */
  boolean apply(ParsedEvent event) {
    Boolean result =
        tx.execute(
            status -> {
              int inserted =
                  jdbc.update(
                      "INSERT INTO inbox_events(event_id) VALUES (?) ON CONFLICT (event_id) DO NOTHING",
                      event.eventId());
              if (inserted == 0) {
                jdbc.update(
                    "INSERT INTO consumer_attempts(event_id, status) VALUES (?,?)",
                    event.eventId(),
                    "DUPLICATE");
                return false;
              }
              jdbc.update(
                  "INSERT INTO business_state(event_id, amount, note) VALUES (?,?,?)",
                  event.eventId(),
                  event.amount(),
                  event.note());
              jdbc.update(
                  "INSERT INTO consumer_attempts(event_id, status) VALUES (?,?)",
                  event.eventId(),
                  "APPLIED");
              return true;
            });
    return Boolean.TRUE.equals(result);
  }

  static ParsedEvent parse(String key, String value) {
    if (key == null) {
      throw new PoisonRecordException("missing record key");
    }
    UUID eventId;
    try {
      eventId = UUID.fromString(key);
    } catch (IllegalArgumentException e) {
      throw new PoisonRecordException("record key is not a UUID: " + key, e);
    }
    if (value == null) {
      throw new PoisonRecordException("missing record body");
    }
    JsonNode root;
    try {
      root = MAPPER.readTree(value);
    } catch (Exception e) {
      throw new PoisonRecordException("body is not valid JSON", e);
    }
    if (root == null || !root.isObject()) {
      throw new PoisonRecordException("body is not a JSON object");
    }
    JsonNode amountNode = root.get("amount");
    if (amountNode == null || amountNode.isNull()) {
      throw new PoisonRecordException("missing amount");
    }
    BigDecimal amount;
    try {
      if (amountNode.isNumber()) {
        amount = amountNode.decimalValue();
      } else if (amountNode.isTextual()) {
        amount = new BigDecimal(amountNode.asText().trim());
      } else {
        throw new PoisonRecordException("amount is not numeric");
      }
    } catch (NumberFormatException e) {
      throw new PoisonRecordException("amount is not numeric", e);
    }
    if (amount.stripTrailingZeros().scale() > 2
        || amount.precision() - amount.scale() > MAX_INTEGER_DIGITS) {
      throw new PoisonRecordException("amount out of range for numeric(12,2): " + amount);
    }
    JsonNode noteNode = root.get("note");
    String note = noteNode == null || noteNode.isNull() ? null : noteNode.asText();
    return new ParsedEvent(eventId, amount.setScale(2), note);
  }
}
