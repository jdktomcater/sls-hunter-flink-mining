package com.sls.hunter.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class PostgresSnapshotsTest {
  @Test
  void parsesReplaceSnapshot() {
    String json =
        Json.write(
            Json.obj(
                "schema_version",
                "1.0",
                "kind",
                "SNAPSHOT",
                "operation",
                "REPLACE",
                "index_family",
                "tracing_sas_linux",
                "target_day",
                "2026-06-10",
                "revision",
                3,
                "status",
                "PROVISIONAL",
                "rule_version",
                "java-v1",
                "computed_at",
                "2026-06-10T04:00:00Z",
                "all_signals",
                Collections.singletonList(Json.obj("signal_id", "abc")),
                "deleted_signal_ids",
                Collections.emptyList(),
                "shallow_candidates",
                Collections.emptyList(),
                "stats",
                Json.obj("signals", 1)));
    PostgresSnapshots.Row row = PostgresSnapshots.Row.parse(json);
    assertEquals("tracing_sas_linux", row.indexFamily);
    assertEquals(LocalDate.parse("2026-06-10"), row.targetDay);
    assertEquals(3L, row.revision);
    assertEquals("PROVISIONAL", row.status);
    assertEquals(1, row.signalCount);
    assertEquals(OffsetDateTime.parse("2026-06-10T04:00:00Z"), row.computedAt);
  }

  @Test
  void rejectsNonSnapshot() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresSnapshots.Row.parse("{\"kind\":\"EVENT\",\"operation\":\"REPLACE\"}"));
  }

  @Test
  void parsesFixedWindowSnapshot() {
    String json =
        Json.write(
            Json.obj(
                "schema_version",
                "1.0",
                "kind",
                "SNAPSHOT",
                "operation",
                "REPLACE",
                "index_family",
                "tracing_sas_linux",
                "target_day",
                "2026-06-10",
                "window_start",
                "2026-06-10T02:00:00Z",
                "window_end",
                "2026-06-10T02:05:00Z",
                "revision",
                2,
                "status",
                "FINAL",
                "rule_version",
                "java-v1-legacy-fleet",
                "computed_at",
                "2026-06-10T02:05:01Z",
                "all_signals",
                Collections.singletonList(Json.obj("signal_id", "abc")),
                "stats",
                Json.obj("signals", 1)));
    PostgresSnapshots.FixedRow row = PostgresSnapshots.FixedRow.parse(json);
    assertEquals("tracing_sas_linux", row.indexFamily);
    assertEquals(OffsetDateTime.parse("2026-06-10T02:00:00Z"), row.windowStart);
    assertEquals(OffsetDateTime.parse("2026-06-10T02:05:00Z"), row.windowEnd);
    assertEquals(LocalDate.parse("2026-06-10"), row.targetDay);
    assertEquals(2L, row.revision);
    assertEquals(1, row.signalCount);
  }

  @Test
  void fixedWindowRejectsADaySnapshot() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PostgresSnapshots.FixedRow.parse(
                "{\"kind\":\"SNAPSHOT\",\"operation\":\"REPLACE\",\"index_family\":\"tracing_sas_linux\",\"target_day\":\"2026-06-10\"}"));
  }

  @Test
  void fixedUpsertKeepsNewerRevision() {
    String sql = PostgresSnapshots.fixedUpsert("mining_window_fixed");
    assertTrue(sql.contains("ON CONFLICT (index_family, window_start)"));
    assertTrue(sql.contains("WHERE mining_window_fixed.revision < EXCLUDED.revision"));
    assertTrue(
        PostgresSnapshots.fixedDdl("mining_window_fixed")
            .contains("PRIMARY KEY (index_family, window_start)"));
  }

  @Test
  void upsertKeepsNewerRevision() {
    String sql = PostgresSnapshots.upsert("mining_window");
    assertTrue(sql.contains("ON CONFLICT (index_family, target_day)"));
    assertTrue(sql.contains("WHERE mining_window.revision < EXCLUDED.revision"));
    assertEquals(sql, PostgresSnapshots.upsert(PostgresSnapshots.table("mining_window")));
  }

  @Test
  void rejectsUnsafeTableName() {
    assertThrows(IllegalArgumentException.class, () -> PostgresSnapshots.table("mining_window;drop"));
    assertThrows(IllegalArgumentException.class, () -> PostgresSnapshots.upsert("public.mining_window"));
  }

  @Test
  void redactsPasswordInUrl() {
    assertEquals(
        "jdbc:postgresql://db/sls?user=postgres&password=***&ssl=true",
        PostgresSnapshots.redact(
            "jdbc:postgresql://db/sls?user=postgres&password=secret&ssl=true"));
  }

  @Test
  void connectionUrlFailsFast() {
    assertEquals(
        "jdbc:postgresql://db/sls?connectTimeout=10",
        PostgresSnapshots.connectionUrl("jdbc:postgresql://db/sls"));
    assertEquals(
        "jdbc:postgresql://db/sls?ssl=true&connectTimeout=10",
        PostgresSnapshots.connectionUrl("jdbc:postgresql://db/sls?ssl=true"));
    assertEquals(
        "jdbc:postgresql://db/sls?connectTimeout=3",
        PostgresSnapshots.connectionUrl("jdbc:postgresql://db/sls?connectTimeout=3"));
  }

  @Test
  void transactionTimeoutMustOutliveCheckpointCycle() {
    Properties properties = new Properties();
    properties.setProperty("target.start.date", "2026-06-10");
    properties.setProperty("kafka.transaction.timeout.ms", "1000");
    assertThrows(IllegalArgumentException.class, () -> new MiningConfig(properties));
  }

  @Test
  void jdbcFlagRejectsTypos() {
    Properties properties = new Properties();
    properties.setProperty("target.start.date", "2026-06-10");
    properties.setProperty("jdbc.enabled", "yes");
    MiningConfig config = new MiningConfig(properties);
    assertThrows(IllegalArgumentException.class, config::jdbcEnabled);
  }
}
