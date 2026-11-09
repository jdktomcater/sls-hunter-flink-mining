package com.sls.hunter.flink;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Properties;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.junit.jupiter.api.Test;

class CheckpointFlowTest {
  @Test
  void configuresHybridCheckpointsAndSupportsAlignedRollback() {
    Properties properties = new Properties();
    CheckpointConfig checkpoint = new CheckpointConfig();
    MiningJob.configureCheckpointFlow(checkpoint, new MiningConfig(properties));
    assertTrue(checkpoint.isUnalignedCheckpointsEnabled());
    assertEquals(10000L, checkpoint.getMinPauseBetweenCheckpoints());
    assertEquals(Duration.ofSeconds(10), checkpoint.getAlignedCheckpointTimeout());
    assertEquals(1, checkpoint.getMaxConcurrentCheckpoints());
    properties.setProperty("checkpoint.unaligned.enabled", "false");
    properties.setProperty("checkpoint.min.pause.ms", "2000");
    MiningJob.configureCheckpointFlow(checkpoint, new MiningConfig(properties));
    assertFalse(checkpoint.isUnalignedCheckpointsEnabled());
    assertEquals(2000L, checkpoint.getMinPauseBetweenCheckpoints());
  }

  @Test
  void rejectsInvalidBoundsAndInsufficientTransactionTimeout() {
    for (String key : new String[] {"flush.max.events", "flush.max.ms"}) {
      Properties properties = new Properties();
      properties.setProperty(key, "0");
      assertThrows(IllegalArgumentException.class, () -> new MiningConfig(properties));
    }
    Properties properties = new Properties();
    properties.setProperty("checkpoint.unaligned.enabled", "typo");
    assertThrows(IllegalArgumentException.class, () -> new MiningConfig(properties));
    properties.setProperty("checkpoint.unaligned.enabled", "true");
    properties.setProperty("checkpoint.min.pause.ms", "60000");
    assertThrows(IllegalArgumentException.class, () -> new MiningConfig(properties));
  }
}
