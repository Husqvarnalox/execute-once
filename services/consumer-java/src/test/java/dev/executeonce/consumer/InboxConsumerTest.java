package dev.executeonce.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class InboxConsumerTest {

  private static final String KEY = UUID.randomUUID().toString();

  @Test
  void parsesValidRecordWithBigDecimalAmount() {
    ParsedEvent e = InboxConsumer.parse(KEY, "{\"amount\": 19.90, \"note\": \"coffee\"}");
    assertThat(e.eventId()).hasToString(KEY);
    assertThat(e.amount()).isEqualByComparingTo(new BigDecimal("19.90"));
    assertThat(e.note()).isEqualTo("coffee");
  }

  @Test
  void noteIsOptional() {
    assertThat(InboxConsumer.parse(KEY, "{\"amount\": 5}").note()).isNull();
  }

  @Test
  void amountAsNumericStringIsAccepted() {
    assertThat(InboxConsumer.parse(KEY, "{\"amount\": \"7.25\"}").amount())
        .isEqualByComparingTo("7.25");
  }

  @Test
  void invalidKeyIsPoison() {
    assertThatThrownBy(() -> InboxConsumer.parse("not-a-uuid", "{\"amount\":1}"))
        .isInstanceOf(PoisonRecordException.class);
    assertThatThrownBy(() -> InboxConsumer.parse(null, "{\"amount\":1}"))
        .isInstanceOf(PoisonRecordException.class);
  }

  @Test
  void invalidJsonIsPoison() {
    assertThatThrownBy(() -> InboxConsumer.parse(KEY, "{oops"))
        .isInstanceOf(PoisonRecordException.class);
    assertThatThrownBy(() -> InboxConsumer.parse(KEY, null))
        .isInstanceOf(PoisonRecordException.class);
    assertThatThrownBy(() -> InboxConsumer.parse(KEY, "[1,2]"))
        .isInstanceOf(PoisonRecordException.class);
  }

  @Test
  void missingOrNonNumericAmountIsPoison() {
    for (String body :
        new String[] {
          "{}",
          "{\"amount\":null}",
          "{\"amount\":\"abc\"}",
          "{\"amount\":true}",
          "{\"amount\":{}}",
          "{\"amount\":1.234}",
          "{\"amount\":12345678901}"
        }) {
      assertThatThrownBy(() -> InboxConsumer.parse(KEY, body))
          .as(body)
          .isInstanceOf(PoisonRecordException.class);
    }
  }
}
