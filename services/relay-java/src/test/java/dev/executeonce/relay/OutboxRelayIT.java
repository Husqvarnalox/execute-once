package dev.executeonce.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

@SpringBootTest(
    properties = {
      "relay.rate-limit-per-second=0",
      "relay.poll-interval=200ms",
      "spring.kafka.admin.fail-fast=true"
    })
@Testcontainers
class OutboxRelayIT {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  @Container @ServiceConnection
  static final RedpandaContainer REDPANDA =
      new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v24.2.7");

  @BeforeAll
  static void migrate() throws Exception {
    List<Path> files;
    try (Stream<Path> s = Files.list(Path.of("../../migrations"))) {
      files = s.filter(p -> p.toString().endsWith(".sql")).sorted().toList();
    }
    try (var con = POSTGRES.createConnection("")) {
      for (Path f : files) {
        try (var st = con.createStatement()) {
          st.execute(Files.readString(f));
        }
      }
    }
  }

  @Autowired JdbcTemplate jdbc;
  @Autowired OutboxCleanup cleanup;

  @Test
  void publishesPendingRowToKafka() throws IOException {
    UUID id = UUID.randomUUID();
    jdbc.update(
        "INSERT INTO outbox_events(event_id, event_type, payload, status) VALUES (?, 'T', ?::jsonb, 'PENDING')",
        id,
        "{\"amount\": 12.34, \"note\": \"hi\"}");

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        jdbc.queryForObject(
                            "SELECT status FROM outbox_events WHERE event_id = ?",
                            String.class,
                            id))
                    .isEqualTo("PUBLISHED"));

    Map<String, Object> row =
        jdbc.queryForMap("SELECT attempt, published_at FROM outbox_events WHERE event_id = ?", id);
    assertThat(row.get("attempt")).isEqualTo(1);
    assertThat(row.get("published_at")).isNotNull();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM relay_attempts WHERE event_id = ? AND status='SUCCESS' AND attempt = 1",
                Integer.class,
                id))
        .isEqualTo(1);

    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
      consumer.subscribe(List.of("events"));
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                  if (id.toString().equals(r.key())) {
                    assertThat(r.value()).contains("12.34");
                    return;
                  }
                }
                throw new AssertionError("record not yet consumed");
              });
    }
  }

  @Test
  void cleanupDeletesOldPublishedRows() {
    UUID old = UUID.randomUUID();
    UUID fresh = UUID.randomUUID();
    jdbc.update(
        "INSERT INTO outbox_events(event_id, event_type, payload, status, attempt, published_at)"
            + " VALUES (?, 'T', '{}'::jsonb, 'PUBLISHED', 1, now() - interval '30 days')",
        old);
    jdbc.update(
        "INSERT INTO outbox_events(event_id, event_type, payload, status, attempt, published_at)"
            + " VALUES (?, 'T', '{}'::jsonb, 'PUBLISHED', 1, now())",
        fresh);

    int deleted = cleanup.cleanup();

    assertThat(deleted).isGreaterThanOrEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE event_id = ?", Integer.class, old))
        .isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE event_id = ?", Integer.class, fresh))
        .isEqualTo(1);
  }
}
