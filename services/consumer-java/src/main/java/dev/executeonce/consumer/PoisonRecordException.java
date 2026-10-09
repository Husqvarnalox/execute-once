package dev.executeonce.consumer;

/** A record that can never be processed successfully; goes straight to the dead-letter topic. */
public class PoisonRecordException extends RuntimeException {
  public PoisonRecordException(String message) {
    super(message);
  }

  public PoisonRecordException(String message, Throwable cause) {
    super(message, cause);
  }
}
