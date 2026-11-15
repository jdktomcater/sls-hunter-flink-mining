package com.sls.hunter.flink;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded window parts plus a separate identity index. Appending never reads or rewrites completed
 * parts. Rule evaluation merges parts using the same counts, full features and bounded samples.
 *
 * <p>The v1 bucket remains a read-only base for restored windows (apart from the existing history
 * compaction). Its identities are indexed once, on first append. All migration writes happen in the
 * operator callback, so checkpoints cannot capture a partially indexed base.
 */
final class WindowBucketState {
    private static final Logger LOG = LoggerFactory.getLogger(WindowBucketState.class);
    static final int PART_EVENTS = 128;
    private final MapState<Long, String> legacy;
    private final MapState<Long, String> headers;
    private final MapState<Tuple2<Long, Long>, String> parts;
    private final MapState<Tuple2<Long, String>, Boolean> identities;
    private final boolean history;

    WindowBucketState(RuntimeContext context, boolean history) {
        this.history = history;
        legacy = context.getMapState(new MapStateDescriptor<>(
                "fixed-window-bucket-v1", Types.LONG, Types.STRING));
        headers = context.getMapState(new MapStateDescriptor<>(
                "fixed-window-parts-header-v1", Types.LONG, Types.STRING));
        parts = context.getMapState(new MapStateDescriptor<>(
                "fixed-window-parts-v1", Types.TUPLE(Types.LONG, Types.LONG), Types.STRING));
        identities = context.getMapState(new MapStateDescriptor<>(
                "fixed-window-identities-v1", Types.TUPLE(Types.LONG, Types.STRING), Types.BOOLEAN));
    }

    boolean contains(long start) throws Exception {
        return headers.contains(start) || legacy.contains(start);
    }

    Iterable<Long> keys() throws Exception {
        Set<Long> keys = new LinkedHashSet<>();
        for (Long start : legacy.keys()) keys.add(start);
        for (Long start : headers.keys()) keys.add(start);
        return keys;
    }

    boolean add(long start, Event event, int limit) throws Exception {
        long started = System.nanoTime();
        try {
            return append(start, event, limit);
        } finally {
            long elapsed = (System.nanoTime() - started) / 1_000_000;
            if (elapsed >= 1000) {
                LOG.warn("Slow fixed-window state write: family={}, windowStart={}, elapsedMillis={}",
                        event.family, start, elapsed);
            }
        }
    }

    private boolean append(long start, Event event, int limit) throws Exception {
        Header header = header(start);
        if (header == null) {
            DayBucket base = readLegacy(start);
            header = new Header();
            header.compact = history;
            header.identities = base.identities.size();
            for (String id : base.identities) identities.put(Tuple2.of(start, id), true);
            headers.put(start, Json.write(header));
        }
        Tuple2<Long, String> identity = Tuple2.of(start, event.id);
        if (identities.contains(identity)) return false;
        if (header.identities >= limit) {
            LOG.error("Fixed-window identity capacity exceeded: family={}, windowStart={}, identities={}, limit={}",
                    event.family, start, header.identities + 1, limit);
            throw new IllegalStateException("Fixed-window identity capacity exceeded");
        }
        long partIndex = header.additions / PART_EVENTS;
        Tuple2<Long, Long> key = Tuple2.of(start, partIndex);
        String stored = parts.get(key);
        DayBucket part = stored == null ? new DayBucket() : Json.MAPPER.readValue(stored, DayBucket.class);
        if (history) {
            part.addWindowHistory(event);
            WindowMining.recordHighRisk(part, event);
        } else {
            part.add(event, false);
        }
        parts.put(key, Json.write(part));
        identities.put(identity, true);
        header.additions++;
        header.identities++;
        headers.put(start, Json.write(header));
        return true;
    }

    DayBucket read(long start) throws Exception {
        Header header = header(start);
        DayBucket bucket = readLegacy(start);
        if (header != null) {
            for (long index = 0; index < partCount(header); index++) {
                bucket.mergeWindowPart(readPart(start, index));
            }
        }
        return bucket;
    }

    void remove(long start) throws Exception {
        String old = legacy.get(start);
        if (old != null) {
            // Cleanup must also work for incompatible/expired state, without evaluating its rules.
            DayBucket base = Json.MAPPER.readValue(old, DayBucket.class);
            for (String id : base.identities) identities.remove(Tuple2.of(start, id));
            legacy.remove(start);
        }
        String rawHeader = headers.get(start);
        if (rawHeader != null) {
            Header header = Json.MAPPER.readValue(rawHeader, Header.class);
            for (long index = 0; index < partCount(header); index++) {
                DayBucket part = readPart(start, index);
                for (String id : part.identities) identities.remove(Tuple2.of(start, id));
                parts.remove(Tuple2.of(start, index));
            }
            headers.remove(start);
        }
    }

    private Header header(long start) throws Exception {
        String stored = headers.get(start);
        if (stored == null) return null;
        Header header = Json.MAPPER.readValue(stored, Header.class);
        if (!history && header.compact) {
            throw new IllegalStateException("Compact fixed-window history state cannot be used by full rules; replay input into fresh state");
        }
        if (history && !header.compact) {
            // Match the previous lazy history compaction; do not invent historical high-risk matches.
            readLegacy(start);
            for (long index = 0; index < partCount(header); index++) {
                DayBucket part = readPart(start, index);
                if (part.compactWindowHistory()) parts.put(Tuple2.of(start, index), Json.write(part));
            }
            header.compact = true;
            headers.put(start, Json.write(header));
        }
        return header;
    }

    private DayBucket readLegacy(long start) throws Exception {
        String stored = legacy.get(start);
        DayBucket base = stored == null ? new DayBucket() : Json.MAPPER.readValue(stored, DayBucket.class);
        base.requireCompatibleBehaviorState();
        if (history && stored != null && base.compactWindowHistory()) {
            legacy.put(start, Json.write(base));
        }
        if (!history && Boolean.TRUE.equals(base.windowHistoryCompact)) {
            throw new IllegalStateException("Compact fixed-window history state cannot be used by full rules; replay input into fresh state");
        }
        return base;
    }

    private DayBucket readPart(long start, long index) throws Exception {
        String stored = parts.get(Tuple2.of(start, index));
        if (stored == null) throw new IllegalStateException("Missing fixed-window state part");
        return Json.MAPPER.readValue(stored, DayBucket.class);
    }

    private static long partCount(Header header) {
        return (header.additions + PART_EVENTS - 1) / PART_EVENTS;
    }

    public static final class Header {
        public long additions;
        public long identities;
        public boolean compact;
    }
}
