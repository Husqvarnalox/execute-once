package dev.executeonce.relay;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
public class OutboxRepository {

  static final int MAX_ERROR_LENGTH = 1000;

  private static final RowMapper<OutboxEvent> MAPPER =
      (rs, i) ->
          new OutboxEvent(
              (UUID) rs.getObject("event_id"),
              rs.getString("idem_key"),
              rs.getString("event_type"),
              rs.getString("payload_json"),
              rs.getString("status"),
              rs.getInt("attempt"),
              rs.getObject("next_attempt_at", OffsetDateTime.class));

  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;

  public OutboxRepository(JdbcTemplate jdbc, TransactionTemplate tx) {
    this.jdbc = jdbc;
    this.tx = tx;
  }

  /**
   * Claims up to {@code batchSize} due PENDING rows with FOR UPDATE SKIP LOCKED and pushes their
   * next_attempt_at forward by {@code lease} in a single UPDATE, all in one transaction.
   */
  public List<OutboxEvent> claimBatch(int batchSize, Duration lease) {
    return tx.execute(
        status -> {
          List<OutboxEvent> events =
              jdbc.query(
                  "SELECT event_id, idem_key, event_type, payload::text AS payload_json, status,"
                      + " attempt, next_attempt_at FROM outbox_events"
                      + " WHERE status = 'PENDING' AND next_attempt_at <= now()"
                      + " ORDER BY next_attempt_at"
                      + " FOR UPDATE SKIP LOCKED LIMIT ?",
                  MAPPER,
                  batchSize);
          if (!events.isEmpty()) {
            UUID[] ids = events.stream().map(OutboxEvent::eventId).toArray(UUID[]::new);
            jdbc.update(
                con -> {
                  var ps =
                      con.prepareStatement(
                          "UPDATE outbox_events SET next_attempt_at = now() +"
                              + " make_interval(secs => ?) WHERE event_id = ANY(?)");
                  ps.setDouble(1, lease.toMillis() / 1000.0);
                  ps.setArray(2, con.createArrayOf("uuid", ids));
                  return ps;
                });
          }
          return events;
        });
  }

  /** Makes claimed-but-unsent rows eligible again right away instead of after the lease. */
  public void releaseLease(List<OutboxEvent> events) {
    if (events.isEmpty()) {
      return;
    }
    UUID[] ids = events.stream().map(OutboxEvent::eventId).toArray(UUID[]::new);
    jdbc.update(
        con -> {
          var ps =
              con.prepareStatement(
                  "UPDATE outbox_events SET next_attempt_at = now()"
                      + " WHERE event_id = ANY(?) AND status = 'PENDING'");
          ps.setArray(1, con.createArrayOf("uuid", ids));
          return ps;
        });
  }

  public void markPublished(OutboxEvent event) {
    tx.executeWithoutResult(
        status -> {
          jdbc.update(
              "UPDATE outbox_events SET status = 'PUBLISHED', attempt = attempt + 1,"
                  + " published_at = now() WHERE event_id = ?",
              event.eventId());
          recordAttempt(event.eventId(), event.attempt() + 1, "SUCCESS", null);
        });
  }

  public void scheduleRetry(OutboxEvent event, Duration delay, String error) {
    tx.executeWithoutResult(
        status -> {
          jdbc.update(
              "UPDATE outbox_events SET attempt = attempt + 1,"
                  + " next_attempt_at = now() + make_interval(secs => ?) WHERE event_id = ?",
              delay.toMillis() / 1000.0,
              event.eventId());
          recordAttempt(event.eventId(), event.attempt() + 1, "ERROR", error);
        });
  }

  public void markFailed(OutboxEvent event, String error) {
    tx.executeWithoutResult(
        status -> {
          jdbc.update(
              "UPDATE outbox_events SET status = 'FAILED', attempt = attempt + 1"
                  + " WHERE event_id = ?",
              event.eventId());
          recordAttempt(event.eventId(), event.attempt() + 1, "ERROR", error);
        });
  }

  public void recordAttempt(UUID eventId, int attempt, String status, String error) {
    jdbc.update(
        "INSERT INTO relay_attempts(event_id, attempt, status, error) VALUES (?,?,?,?)",
        eventId,
        attempt,
        status,
        truncate(error));
  }

  public long countBacklog() {
    Long n =
        jdbc.queryForObject(
            "SELECT count(*) FROM outbox_events WHERE status = 'PENDING'", Long.class);
    return n == null ? 0 : n;
  }

  public int deletePublishedOlderThan(Duration age, int limit) {
    return jdbc.update(
        "DELETE FROM outbox_events WHERE event_id IN (SELECT event_id FROM outbox_events"
            + " WHERE status = 'PUBLISHED' AND published_at < now() - make_interval(secs => ?)"
            + " LIMIT ?)",
        age.toSeconds(),
        limit);
  }

  public int deleteIdempotencyKeysOlderThan(Duration age, int limit) {
    return jdbc.update(
        "DELETE FROM idempotency_keys WHERE idem_key IN (SELECT k.idem_key FROM idempotency_keys k"
            + " WHERE k.created_at < now() - make_interval(secs => ?)"
            + " AND NOT EXISTS (SELECT 1 FROM outbox_events o WHERE o.idem_key = k.idem_key)"
            + " LIMIT ?)",
        age.toSeconds(),
        limit);
  }

  static String truncate(String s) {
    if (s == null) {
      return null;
    }
    return s.length() <= MAX_ERROR_LENGTH ? s : s.substring(0, MAX_ERROR_LENGTH);
  }
}
