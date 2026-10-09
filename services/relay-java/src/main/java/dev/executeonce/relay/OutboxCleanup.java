package dev.executeonce.relay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxCleanup {

  private static final Logger log = LoggerFactory.getLogger(OutboxCleanup.class);

  private final OutboxRepository repository;
  private final RelayProperties props;

  public OutboxCleanup(OutboxRepository repository, RelayProperties props) {
    this.repository = repository;
    this.props = props;
  }

  @Scheduled(
      fixedDelayString = "${relay.cleanup-interval:10m}",
      initialDelayString = "${relay.cleanup-interval:10m}")
  public void scheduledCleanup() {
    try {
      cleanup();
    } catch (Exception ex) {
      log.warn("cleanup failed: {}", ex.toString());
    }
  }

  /** Deletes expired PUBLISHED outbox rows, then orphaned idempotency keys. Returns total rows. */
  public int cleanup() {
    int outbox =
        drain(
            () -> repository.deletePublishedOlderThan(props.retention(), props.cleanupBatchSize()));
    int keys =
        drain(
            () ->
                repository.deleteIdempotencyKeysOlderThan(
                    props.retention(), props.cleanupBatchSize()));
    if (outbox > 0 || keys > 0) {
      log.info("cleanup deleted {} outbox rows and {} idempotency keys", outbox, keys);
    }
    return outbox + keys;
  }

  private int drain(java.util.function.IntSupplier batch) {
    int total = 0;
    int n;
    do {
      n = batch.getAsInt();
      total += n;
    } while (n >= props.cleanupBatchSize());
    return total;
  }
}
