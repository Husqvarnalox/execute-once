package dev.executeonce.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
      "consumer.rate-limit-per-second=0",
      "consumer.retry-backoff=100ms",
      "consumer.concurrency=1"
    })
@Testcontainers
class InboxConsumerIT {

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

  private void send(String key, String value) throws Exception {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
      producer.send(new ProducerRecord<>("events", key, value)).get();
    }
  }

  private int count(String sql, Object... args) {
    return jdbc.queryForObject(sql, Integer.class, args);
  }

  @Test
  void appliesOnceAndRecordsDuplicate() throws Exception {
    UUID id = UUID.randomUUID();
    String body = "{\"amount\": 42.50, \"note\": \"first\"}";

    send(id.toString(), body);
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        count(
                            "SELECT count(*) FROM consumer_attempts WHERE event_id=? AND status='APPLIED'",
                            id))
                    .isEqualTo(1));
    BigDecimal amount =
        jdbc.queryForObject(
            "SELECT amount FROM business_state WHERE event_id=?", BigDecimal.class, id);
    assertThat(amount).isEqualByComparingTo("42.50");

    send(id.toString(), body);
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        count(
                            "SELECT count(*) FROM consumer_attempts WHERE event_id=? AND status='DUPLICATE'",
                            id))
                    .isEqualTo(1));
    assertThat(count("SELECT count(*) FROM business_state WHERE event_id=?", id)).isEqualTo(1);
    assertThat(count("SELECT count(*) FROM inbox_events WHERE event_id=?", id)).isEqualTo(1);
  }

  @Test
  void poisonRecordGoesToDlq() throws Exception {
    String badKey = "not-a-uuid-" + UUID.randomUUID();
    send(badKey, "{\"amount\": 1}");

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        count(
                            "SELECT count(*) FROM consumer_attempts WHERE status='DEAD_LETTERED' AND error LIKE ?",
                            "key=" + badKey + "%"))
                    .isEqualTo(1));

    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
      consumer.subscribe(List.of("events.dlq"));
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                  if (badKey.equals(r.key())) {
                    return;
                  }
                }
                throw new AssertionError("record not yet on DLQ");
              });
    }
  }
}
