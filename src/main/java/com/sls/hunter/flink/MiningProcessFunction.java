package com.sls.hunter.flink;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个索引族一个键。族内的日桶、快照、修订号和定时器都在托管状态里，一次刷新统一计算该族所有就绪日期。
 *
 * <p>输出是整日快照而不是单条告警。{@code PROVISIONAL} 表示目标日尚未过完迟到期；封账且过了迟到期后为 {@code FINAL}； 其后内容再变则为 {@code
 * CORRECTED}。
 */
public class MiningProcessFunction extends KeyedProcessFunction<String, Event, String> {
  private static final Logger LOG = LoggerFactory.getLogger(MiningProcessFunction.class);
  public static final OutputTag<String> LATE = new OutputTag<String>("late-events") {};
  private final MiningConfig config;
  private transient MapState<String, String> buckets;
  private transient MapState<String, String> outputs;
  private transient MapState<String, String> fleetOverrides;
  private transient MapState<String, String> pendingEvents;
  private transient MapState<String, Long> revisions;
  private transient ValueState<String> highDay;
  private transient ValueState<Long> pendingTimer;

  public MiningProcessFunction(MiningConfig config) {
    this.config = config;
  }

  @Override
  public void open(Configuration ignored) throws Exception {
    LOG.info(
        "Opening mining operator: subtask={}, baselineDays={}, historyCompare={}, retentionDays={}, emitMillis={}",
        getRuntimeContext().getIndexOfThisSubtask(),
        config.baselineDays,
        config.historyCompare,
        config.retentionDays,
        config.emitMillis);
    pendingEvents = mapState("pending-events-v1", String.class);
    fleetOverrides = mapState("legacy-fleet-v1", String.class);
    buckets = mapState("daily-buckets-v1", String.class);
    outputs = mapState("last-snapshot-v1", String.class);
    revisions =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<String, Long>(
                    "snapshot-revision-v1", String.class, Long.class));
    highDay =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<String>("highest-day-v1", String.class));
    pendingTimer =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<Long>("pending-timer-v1", Long.class));
  }

  private <T> MapState<String, T> mapState(String name, Class<T> valueClass) throws Exception {
    return getRuntimeContext()
        .getMapState(new MapStateDescriptor<String, T>(name, String.class, valueClass));
  }

  private DayBucket readBucket(String day) throws Exception {
    String stored = buckets.get(day);
    return stored == null ? new DayBucket() : Json.MAPPER.readValue(stored, DayBucket.class);
  }

  @Override
  public void processElement(Event event, Context ctx, Collector<String> out) throws Exception {
    if (event.kind.equals("FLEET")) {
      fleetOverrides.put(event.day, Json.write(event.fields));
      LOG.info(
          "Fleet snapshot updated: family={}, day={}, entries={}",
          ctx.getCurrentKey(),
          event.day,
          event.fields.size());
      schedule(ctx.timerService(), ctx.getCurrentKey());
      return;
    }
    LocalDate day = LocalDate.parse(event.day);
    String highest = highDay.value();
    LocalDate top = highest == null ? day : LocalDate.parse(highest);
    if (day.isBefore(retentionStart(top))) {
      LOG.info(
          "Input routed to late output: family={}, kind={}, day={}, id={}, retentionStart={}, highestDay={}, stage=ingress",
          ctx.getCurrentKey(),
          event.kind,
          event.day,
          event.id,
          retentionStart(top),
          top);
      ctx.output(LATE, Json.write(Json.obj("reason", "outside_online_retention", "event", event)));
      return;
    }
    boolean retentionAdvanced = highest == null || day.isAfter(top);
    if (day.isAfter(top)) {
      top = day;
      highDay.update(event.day);
    } else if (highest == null) {
      highDay.update(event.day);
    }
    // 先落入托管状态，再按刷新间隔折进日桶。未刷新的批次随 checkpoint 保留。
    // 同一天、同一种类、同一身份只留最新一条。
    String pendingKey = Json.write(J8.list(event.day, event.kind, event.id));
    boolean replacedPending = pendingEvents.contains(pendingKey);
    pendingEvents.put(pendingKey, Json.write(event));
    LOG.debug(
        "Input buffered: family={}, kind={}, day={}, id={}, highestDay={}, replacedPending={}",
        ctx.getCurrentKey(),
        event.kind,
        event.day,
        event.id,
        top,
        replacedPending);
    // 保留窗口跟着已观察到的最新业务日走，不跟墙钟，历史回放才不会刚读入就被清掉。
    List<String> expired = new ArrayList<>();
    if (retentionAdvanced) {
      LocalDate cutoff = retentionStart(top);
      for (String storedDay : buckets.keys()) {
        if (LocalDate.parse(storedDay).isBefore(cutoff)) {
          expired.add(storedDay);
        }
      }
    }
    for (String storedDay : expired) {
      buckets.remove(storedDay);
      outputs.remove(storedDay);
      revisions.remove(storedDay);
      fleetOverrides.remove(storedDay);
    }
    if (!expired.isEmpty()) {
      LOG.info("Expired day state removed: family={}, days={}", ctx.getCurrentKey(), expired);
    }
    schedule(ctx.timerService(), ctx.getCurrentKey());
  }

  /** 保留窗口的第一天：最新业务日往前 retentionDays-1 天。 */
  private LocalDate retentionStart(LocalDate newest) {
    return newest.minusDays(config.retentionDays - 1L);
  }

  /** 登记下一次处理时间定时器。已有更早的定时器时保持不动，因此封账定时器不会被随后的事件推迟，连续事件也不会把批次一直往后推。 */
  private void schedule(TimerService timers, String family) throws Exception {
    scheduleAt(timers, family, timers.currentProcessingTime() + config.emitMillis, "flush");
  }

  private void scheduleAt(TimerService timers, String family, long time, String purpose)
      throws Exception {
    Long current = pendingTimer.value();
    if (current == null || time < current) {
      if (current != null) {
        timers.deleteProcessingTimeTimer(current);
      }
      timers.registerProcessingTimeTimer(time);
      pendingTimer.update(time);
      LOG.info(
          "Processing timer registered: family={}, purpose={}, fireAt={}, replacedFireAt={}",
          family,
          purpose,
          time,
          current);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    long started = System.nanoTime();
    LOG.info("Mining flush started: family={}, processingTime={}", ctx.getCurrentKey(), timestamp);
    pendingTimer.clear();
    List<String> drainedKeys = new ArrayList<>();
    Map<String, DayBucket> touched = drainPending(ctx, drainedKeys);
    for (Map.Entry<String, DayBucket> entry : touched.entrySet()) {
      buckets.put(entry.getKey(), Json.write(entry.getValue()));
    }
    // 日桶和对应待处理项的删除在同一回调完成，checkpoint 不会看到半个批次。
    for (String key : drainedKeys) {
      pendingEvents.remove(key);
    }
    if (!pendingEvents.isEmpty()) {
      scheduleAt(
          ctx.timerService(),
          ctx.getCurrentKey(),
          ctx.timerService().currentProcessingTime() + 1,
          "continue-flush");
      LOG.info(
          "Mining flush yielded: family={}, drained={}, elapsedMillis={}",
          ctx.getCurrentKey(),
          drainedKeys.size(),
          (System.nanoTime() - started) / 1_000_000);
      return;
    }
    Map<String, DayBucket> days = new TreeMap<>();
    for (Map.Entry<String, String> entry : buckets.entries()) {
      DayBucket updated = touched.get(entry.getKey());
      days.put(
          entry.getKey(),
          updated == null ? Json.MAPPER.readValue(entry.getValue(), DayBucket.class) : updated);
    }
    long nextFinal = publishReadyDays(timestamp, ctx, out, days);
    if (nextFinal != Long.MAX_VALUE && nextFinal > timestamp) {
      scheduleAt(ctx.timerService(), ctx.getCurrentKey(), nextFinal, "final");
    }
    LOG.info(
        "Mining flush completed: family={}, touchedDays={}, retainedDays={}, elapsedMillis={}",
        ctx.getCurrentKey(),
        touched.size(),
        days.size(),
        (System.nanoTime() - started) / 1_000_000);
  }

  /** 把待处理事件折进各自的日桶。刷新时已经滑出保留窗口的事件改送迟到侧输出。 */
  private Map<String, DayBucket> drainPending(OnTimerContext ctx, List<String> drainedKeys)
      throws Exception {
    Map<String, DayBucket> touched = new TreeMap<>();
    String highest = highDay.value();
    LocalDate cutoff = highest == null ? LocalDate.MIN : retentionStart(LocalDate.parse(highest));
    int processed = 0, accepted = 0, skipped = 0, late = 0, seals = 0;
    long started = System.nanoTime();
    for (Map.Entry<String, String> pending : pendingEvents.entries()) {
      if (processed >= config.flushMaxEvents
          || (processed > 0
              && (System.nanoTime() - started) / 1_000_000 >= config.flushMaxMillis)) {
        break;
      }
      processed++;
      drainedKeys.add(pending.getKey());
      Event event = Json.MAPPER.readValue(pending.getValue(), Event.class);
      if (LocalDate.parse(event.day).isBefore(cutoff)) {
        late++;
        LOG.info(
            "Input routed to late output: family={}, kind={}, day={}, id={}, retentionStart={}, stage=flush",
            ctx.getCurrentKey(),
            event.kind,
            event.day,
            event.id,
            cutoff);
        ctx.output(
            LATE,
            Json.write(Json.obj("reason", "outside_online_retention_at_flush", "event", event)));
        continue;
      }
      DayBucket bucket = touched.get(event.day);
      if (bucket == null) {
        bucket = readBucket(event.day);
        touched.put(event.day, bucket);
      }
      // 同一批里的 SEAL 只把这一天标成封账，不丢弃同批的事件。
      if (event.kind.equals("SEAL")) {
        if (bucket.sealed) {
          LOG.info(
              "Seal ignored because day already sealed: family={}, day={}",
              ctx.getCurrentKey(),
              event.day);
        } else {
          LOG.info(
              "Day sealed: family={}, day={}, events={}",
              ctx.getCurrentKey(),
              event.day,
              bucket.events);
        }
        bucket.sealed = true;
        seals++;
      } else if (bucket.identities.contains(event.id)) {
        skipped++;
        LOG.debug(
            "Input skipped: family={}, day={}, id={}, reason=duplicateIdentity",
            ctx.getCurrentKey(),
            event.day,
            event.id);
      } else if (bucket.add(event)) {
        accepted++;
        LOG.debug(
            "Input folded into day bucket: family={}, day={}, id={}, dayEvents={}, sealed={}",
            ctx.getCurrentKey(),
            event.day,
            event.id,
            bucket.events,
            bucket.sealed);
      } else {
        skipped++;
        LOG.debug(
            "Input skipped: family={}, day={}, id={}, reason=dayEndSubsecond",
            ctx.getCurrentKey(),
            event.day,
            event.id);
      }
      if (bucket.identities.size() > config.integer("max.events.per.family.day", 2000000)) {
        LOG.error(
            "Daily identity capacity exceeded: family={}, day={}, identities={}, limit={}",
            ctx.getCurrentKey(),
            event.day,
            bucket.identities.size(),
            config.integer("max.events.per.family.day", 2000000));
        throw new IllegalStateException("Daily identity capacity exceeded");
      }
    }
    LOG.info(
        "Pending batch drained: family={}, processed={}, accepted={}, skipped={}, late={}, seals={}",
        ctx.getCurrentKey(),
        processed,
        accepted,
        skipped,
        late,
        seals);
    if (late > 0) {
      LOG.warn(
          "Pending events routed to late output: family={}, count={}, retentionStart={}",
          ctx.getCurrentKey(),
          late,
          cutoff);
    }
    return touched;
  }

  /** 打开历史对比时，基线日都已封账且 fleet 就绪才计算。关掉后当天桶直接按规则筛选。目标日封账且过了结束时间加允许迟到，快照才是终态。内容没变且终态标志也没变时不重复下发。 */
  private long publishReadyDays(
      long timestamp, OnTimerContext ctx, Collector<String> out, Map<String, DayBucket> days)
      throws Exception {
    String family = ctx.getCurrentKey();
    Map<String, Object> familyFleet = familyFleet(family);
    long nextFinal = Long.MAX_VALUE;
    for (Map.Entry<String, DayBucket> entry : days.entrySet()) {
      LocalDate target = LocalDate.parse(entry.getKey());
      if (target.isBefore(config.firstTarget)) {
        LOG.info(
            "Window skipped before first target: family={}, targetDay={}, firstTarget={}",
            family,
            target,
            config.firstTarget);
        continue;
      }
      if (config.historyCompare) {
        String missingDay = unsealedBaselineDay(target, days);
        if (missingDay != null) {
          LOG.info(
              "Window waiting for sealed baseline: family={}, targetDay={}, missingDay={}, baselineDays={}",
              family,
              target,
              missingDay,
              config.baselineDays);
          continue;
        }
      }
      long dayEnd = target.plusDays(1).atStartOfDay(Event.ZONE).toInstant().toEpochMilli();
      boolean complete = entry.getValue().sealed && timestamp >= dayEnd + config.latenessMillis;
      if (!complete && entry.getValue().sealed) {
        nextFinal = Math.min(nextFinal, dayEnd + config.latenessMillis);
      }
      // 兼容快照缺失时跳过该日。未知稀有度不能当成 0 台主机。规则筛选不使用 fleet。
      if (config.historyCompare && !fleetReady(family, entry.getKey(), familyFleet)) {
        LOG.info(
            "Window waiting for fleet snapshot: family={}, targetDay={}, fleetMode={}",
            family,
            target,
            config.get("fleet.mode", "legacy_snapshot"));
        continue;
      }
      long evaluationStarted = System.nanoTime();
      MiningEngine.Result result =
          MiningEngine.evaluate(family, target, days, evaluationConfig(family, familyFleet));
      LOG.info(
          "Window evaluated: family={}, targetDay={}, dayEvents={}, sealed={}, signals={}, shallowCandidates={}, complete={}, elapsedMillis={}",
          family,
          target,
          entry.getValue().events,
          entry.getValue().sealed,
          result.signals().size(),
          result.shallow().size(),
          complete,
          (System.nanoTime() - evaluationStarted) / 1_000_000);
      if (unchanged(entry.getKey(), result, complete)) {
        LOG.info(
            "Unchanged snapshot suppressed: family={}, targetDay={}, complete={}",
            family,
            target,
            complete);
        continue;
      }
      emit(timestamp, ctx, out, entry.getKey(), target, result, complete);
    }
    return nextFinal;
  }

  /** 第一个尚未封账的基线日。全部封账时返回 null。 */
  private String unsealedBaselineDay(LocalDate target, Map<String, DayBucket> days) {
    for (int age = 1; age <= config.baselineDays; age++) {
      String day = target.minusDays(age).toString();
      DayBucket baseline = days.get(day);
      if (baseline == null || !baseline.sealed) {
        return day;
      }
    }
    return null;
  }

  /** Nginx 不使用 fleet。exact 模式用桶内去重，不要求外部快照。 */
  private boolean fleetReady(String family, String day, Map<String, Object> familyFleet) {
    if (family.equals("tracing_nginx_hertzbeat")) {
      return true;
    }
    if (!config.get("fleet.mode", "legacy_snapshot").equals("legacy_snapshot")) {
      return true;
    }
    return familyFleet.containsKey(day);
  }

  /** 文件快照打底，流里的 FLEET 按目标日覆盖。拷贝只替换当前族，不改作业级配置。 */
  private Map<String, Object> familyFleet(String family) throws Exception {
    Map<String, Object> overlaid = new LinkedHashMap<>();
    Object initial = config.fleetSnapshots.get(family);
    if (initial instanceof Map) {
      Map<?, ?> snapshot = (Map<?, ?>) initial;
      for (Map.Entry<?, ?> entry : snapshot.entrySet()) {
        overlaid.put(entry.getKey().toString(), entry.getValue());
      }
    }
    for (Map.Entry<String, String> entry : fleetOverrides.entries()) {
      overlaid.put(entry.getKey(), Json.map(entry.getValue()));
    }
    return overlaid;
  }

  private MiningConfig evaluationConfig(String family, Map<String, Object> familyFleet) {
    MiningConfig copy = new MiningConfig(config.properties);
    copy.fleetSnapshots = new HashMap<>(config.fleetSnapshots);
    copy.fleetSnapshots.put(family, familyFleet);
    return copy;
  }

  /** 状态里只存签名、状态和信号 id，不存整份快照。 */
  private boolean unchanged(String day, MiningEngine.Result result, boolean complete)
      throws Exception {
    String previous = outputs.get(day);
    Map<String, Object> stored =
        previous == null ? Collections.<String, Object>emptyMap() : Json.map(previous);
    boolean wasFinal =
        J8.set("FINAL", "CORRECTED").contains(stored.getOrDefault("status", "").toString());
    String signature =
        Json.write(
            Json.obj(
                "signals",
                result.signals(),
                "shallow_candidates",
                result.shallow(),
                "stats",
                result.stats()));
    return signature.equals(stored.get("signature")) && complete == wasFinal;
  }

  private void emit(
      long timestamp,
      OnTimerContext ctx,
      Collector<String> out,
      String day,
      LocalDate target,
      MiningEngine.Result result,
      boolean complete)
      throws Exception {
    String previous = outputs.get(day);
    Map<String, Object> stored =
        previous == null ? Collections.<String, Object>emptyMap() : Json.map(previous);
    boolean wasFinal =
        J8.set("FINAL", "CORRECTED").contains(stored.getOrDefault("status", "").toString());
    String status = complete ? (wasFinal ? "CORRECTED" : "FINAL") : "PROVISIONAL";
    long revision = revisions.contains(day) ? revisions.get(day) + 1 : 1;
    revisions.put(day, revision);
    String family = ctx.getCurrentKey();
    Set<String> currentIds = new TreeSet<>();
    List<Map<String, Object>> decorated = new ArrayList<>();
    for (Map<String, Object> signal : result.signals()) {
      String id = signalId(family, day, signal);
      currentIds.add(id);
      Map<String, Object> copy = new LinkedHashMap<>(signal);
      copy.put("signal_id", id);
      copy.put("target_day", day);
      decorated.add(copy);
    }
    Set<String> deleted = new TreeSet<>();
    Object previousIds = stored.get("ids");
    if (previousIds instanceof List) {
      List<?> ids = (List<?>) previousIds;
      for (Object id : ids) {
        if (!currentIds.contains(id.toString())) {
          deleted.add(id.toString());
        }
      }
    }
    String signature =
        Json.write(
            Json.obj(
                "signals",
                result.signals(),
                "shallow_candidates",
                result.shallow(),
                "stats",
                result.stats()));
    out.collect(
        Json.write(
            Json.obj(
                "schema_version",
                "1.0",
                "kind",
                "SNAPSHOT",
                "operation",
                "REPLACE",
                "index_family",
                family,
                "target_day",
                day,
                "baseline_dates",
                config.historyCompare
                    ? IntStream.rangeClosed(1, config.baselineDays)
                        .mapToObj(age -> target.minusDays(age).toString())
                        .collect(Collectors.toList())
                    : Collections.<String>emptyList(),
                "rule_version",
                config.get("rule.version", "java-v1-exact-fleet"),
                "revision",
                revision,
                "status",
                status,
                "computed_at",
                Instant.ofEpochMilli(timestamp).toString(),
                "all_signals",
                decorated,
                "deleted_signal_ids",
                deleted,
                "shallow_candidates",
                result.shallow(),
                "stats",
                result.stats())));
    outputs.put(
        day, Json.write(Json.obj("signature", signature, "status", status, "ids", currentIds)));
    LOG.info(
        "Snapshot emitted to downstream operator (Kafka commit pending): family={}, targetDay={}, revision={}, status={}, signals={}, shallowCandidates={}, deletedSignals={}",
        family,
        day,
        revision,
        status,
        decorated.size(),
        result.shallow().size(),
        deleted.size());
  }

  /** 身份哈希不含分数，同一实体重新打分后 id 保持不变。 */
  public static String signalId(String family, String targetDay, Map<String, Object> signal) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(
                  (family + "\n" + targetDay + "\n" + MiningEngine.identity(signal))
                      .getBytes(StandardCharsets.UTF_8));
      return J8.hex(digest);
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }
}
