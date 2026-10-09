package dev.executeonce.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Records DEAD_LETTERED attempts outside of any failed business transaction. */
@Component
public class DeadLetterAuditor {

  private static final Logger log = LoggerFactory.getLogger(DeadLetterAuditor.class);
  static final int MAX_ERROR_LENGTH = 1000;

  private final JdbcTemplate jdbc;
  private final Counter deadLettered;

  public DeadLetterAuditor(JdbcTemplate jdbc, MeterRegistry registry) {
    this.jdbc = jdbc;
    this.deadLettered = registry.counter("inbox_dead_lettered_total");
  }

  public void recordDeadLettered(ConsumerRecord<?, ?> record, Exception ex) {
    deadLettered.increment();
    Throwable root = ex;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String error =
        "key=" + record.key() + " " + root.getClass().getSimpleName() + ": " + root.getMessage();
    if (error.length() > MAX_ERROR_LENGTH) {
      error = error.substring(0, MAX_ERROR_LENGTH);
    }
    try {
      jdbc.update(
          "INSERT INTO consumer_attempts(event_id, status, error) VALUES (?,?,?)",
          eventIdFor(record),
          "DEAD_LETTERED",
          error);
    } catch (Exception dbEx) {
      // Never block the dead-letter path on the audit write.
      log.error(
          "could not audit dead-lettered record {}-{}@{}",
          record.topic(),
          record.partition(),
          record.offset(),
          dbEx);
    }
    log.warn(
        "dead-lettered {}-{}@{}: {}", record.topic(), record.partition(), record.offset(), error);
  }

  /** The record key when it is a UUID, otherwise a stable name-based UUID of its coordinates. */
  static UUID eventIdFor(ConsumerRecord<?, ?> record) {
    try {
      return UUID.fromString(String.valueOf(record.key()));
    } catch (IllegalArgumentException e) {
      String coords = record.topic() + "-" + record.partition() + "-" + record.offset();
      return UUID.nameUUIDFromBytes(coords.getBytes(StandardCharsets.UTF_8));
    }
  }
}
