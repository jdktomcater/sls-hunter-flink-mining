package com.sls.hunter.flink;

import static com.sls.hunter.flink.Json.obj;
import static com.sls.hunter.flink.Scoring.adjust;
import static com.sls.hunter.flink.Scoring.invoked;
import static com.sls.hunter.flink.Scoring.pre;
import static com.sls.hunter.flink.Scoring.round;
import static com.sls.hunter.flink.Scoring.score;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * 纯函数评估器，流作业和有界回放共用。输入是已经按天折好的桶，这里不读外部存储。
 *
 * <p>SAS 与 Nginx 是两套规则。SAS 的 {@code baseline_count_max} 虽沿用旧字段名，值是基线窗口的合计次数，不是日峰值。
 */
public final class MiningEngine {
  private MiningEngine() {}

  /** 深挖信号、未进入深挖的浅候选，以及本窗口统计。 */
  public static final class Result {
    private final List<Map<String, Object>> signals;
    private final List<Map<String, Object>> shallow;
    private final Map<String, Object> stats;

    public Result(
        List<Map<String, Object>> signals,
        List<Map<String, Object>> shallow,
        Map<String, Object> stats) {
      this.signals = signals;
      this.shallow = shallow;
      this.stats = stats;
    }

    public List<Map<String, Object>> signals() {
      return signals;
    }

    public List<Map<String, Object>> shallow() {
      return shallow;
    }

    public Map<String, Object> stats() {
      return stats;
    }
  }

  /** 一个三元组候选。{@code pre} 只参与排序和浅候选展示。 */
  private static final class Candidate {
    final String key;
    final String host;
    final String user;
    final String proc;
    final long target;
    final long baseline;
    final boolean novelHost;
    final int fleet;
    final double ratio;
    final double pre;
    final boolean gateway;

    Candidate(
        String key,
        String host,
        String user,
        String proc,
        long target,
        long baseline,
        boolean novelHost,
        int fleet,
        double ratio,
        double pre,
        boolean gateway) {
      this.key = key;
      this.host = host;
      this.user = user;
      this.proc = proc;
      this.target = target;
      this.baseline = baseline;
      this.novelHost = novelHost;
      this.fleet = fleet;
      this.ratio = ratio;
      this.pre = pre;
      this.gateway = gateway;
    }
  }

  /** 基线窗口汇总。父路径会记住片段里的每一个路径，评分时只用第一段里的第一个路径。 */
  private static final class Baseline {
    final Set<String> hosts;
    final Map<String, Long> tripleCounts;
    final Map<String, Set<String>> procHosts;
    final Set<String> parentPaths;

    Baseline(
        Set<String> hosts,
        Map<String, Long> tripleCounts,
        Map<String, Set<String>> procHosts,
        Set<String> parentPaths) {
      this.hosts = hosts;
      this.tripleCounts = tripleCounts;
      this.procHosts = procHosts;
      this.parentPaths = parentPaths;
    }
  }

  public static Result evaluate(
      String family, LocalDate target, Map<String, DayBucket> days, MiningConfig config) {
    DayBucket targetDay = days.getOrDefault(target.toString(), new DayBucket());
    if (!config.historyCompare) {
      return family.equals("tracing_nginx_hertzbeat")
          ? nginxWithoutHistory(targetDay)
          : sasRules(family, targetDay, config);
    }
    List<DayBucket> baselineDays = new ArrayList<>();
    for (int age = 1; age <= config.baselineDays; age++)
      baselineDays.add(days.getOrDefault(target.minusDays(age).toString(), new DayBucket()));
    return family.equals("tracing_nginx_hertzbeat")
        ? nginx(targetDay, baselineDays, config)
        : sas(family, target, targetDay, baselineDays, config);
  }

  private static List<Map<String, Object>> sample(List<Event> events, int limit) {
    return events.stream()
        .limit(limit)
        .map(
            event ->
                obj(
                    "timestamp",
                    event.fields.getOrDefault("timestamp", event.fields.get("@timestamp")),
                    "cmdline",
                    event.s("cmdline"),
                    "cmd_chain",
                    Templates.cut(event.s("cmd_chain"), 600),
                    "dst_ip",
                    event.s("dst_ip")))
        .collect(Collectors.toList());
  }

  private static Map<String, Object> signal(
      String type, String host, String user, String proc, double score) {
    return obj(
        "signal_type", type, "host_ip", host, "username", user, "proc_name", proc, "score", round(score));
  }

  /** Nginx 独立于三元组模型：URI 模板突增，以及基线没见过的 upstream。 */
  private static Result nginx(DayBucket targetDay, List<DayBucket> baselineDays, MiningConfig config) {
    List<Map<String, Object>> signals = new ArrayList<>();
    double spikeRatio = Double.parseDouble(config.get("host.spike.ratio", "2.5"));
    for (Map.Entry<String, DayBucket.Aggregate> entry : targetDay.uris.entrySet()) {
      String uri = entry.getKey();
      DayBucket.Aggregate aggregate = entry.getValue();
      if (aggregate.count < config.minCount) {
        continue;
      }
      long baseline =
          baselineDays.stream()
              .mapToLong(day -> day.uris.getOrDefault(uri, new DayBucket.Aggregate()).count)
              .sum();
      boolean novel = baseline == 0;
      double perDay = baseline / (double) config.baselineDays;
      if (!novel && aggregate.count / perDay < spikeRatio) {
        continue;
      }
      Map<String, Object> row =
          signal(
              novel ? "novel_uri_template" : "uri_template_spike",
              config.get("nginx.index.name", "tracing-nginx-hertzbeat"),
              "",
              Templates.cut(uri, 80),
              uriScore(novel, aggregate.count, uri));
      row.put("entity_key", uri);
      row.put("target_count", aggregate.count);
      row.put("baseline_count_max", baseline);
      row.put(
          "samples",
          aggregate.samples.stream()
              .map(
                  event ->
                      obj(
                          "timestamp",
                          event.fields.get("@timestamp"),
                          "cmdline",
                          event.s("method") + " " + event.s("uri"),
                          "cmd_chain",
                          "status=" + event.s("status") + " upstream=" + event.s("upstream_addr")))
              .collect(Collectors.toList()));
      signals.add(row);
    }
    for (Map.Entry<String, DayBucket.Aggregate> entry : targetDay.upstreams.entrySet()) {
      String upstream = entry.getKey();
      long count = entry.getValue().count;
      if (upstream.isEmpty()
          || count < config.minCount
          || baselineDays.stream().anyMatch(day -> day.upstreams.containsKey(upstream))) {
        continue;
      }
      Map<String, Object> row =
          signal(
              "novel_upstream", upstream.split(":", 2)[0], "", "nginx_upstream", 35 + Math.min(count, 20));
      row.put("entity_key", upstream);
      row.put("target_count", count);
      row.put("baseline_count_max", 0);
      signals.add(row);
    }
    sort(signals);
    // Nginx 只保留 maxSignals 条，没有 SAS 那种额外余量。
    return new Result(
        new ArrayList<>(signals.subList(0, Math.min(signals.size(), config.maxSignals))),
        Collections.<Map<String, Object>>emptyList(),
        obj(
            "target_events",
            targetDay.events,
            "baseline_events",
            baselineDays.stream().mapToLong(day -> day.events).sum(),
            "signals",
            signals.size(),
            "history_compare",
            true));
  }

  /** Nginx 规则全部建立在 URI / upstream 的历史差集上，关掉历史对比后不产出信号。 */
  private static Result nginxWithoutHistory(DayBucket targetDay) {
    return new Result(
        Collections.<Map<String, Object>>emptyList(),
        Collections.<Map<String, Object>>emptyList(),
        obj("target_events", targetDay.events, "signals", 0, "history_compare", false));
  }

  /** 新 URI：40 + 次数（封顶 30）+ 模板长度。旧 URI：30 + 次数/5（封顶 25）。 */
  private static double uriScore(boolean novel, long count, String uri) {
    if (novel) {
      return 40 + Math.min(count * 2, 30) + Math.min(uri.codePointCount(0, uri.length()) / 10.0, 10);
    }
    return 30 + Math.min(count / 5.0, 25);
  }

  private static Result sas(
      String family,
      LocalDate target,
      DayBucket targetDay,
      List<DayBucket> baselineDays,
      MiningConfig config) {
    Baseline baseline = baseline(family, baselineDays);
    List<Candidate> candidates = candidates(family, target, targetDay, baseline, config);
    candidates.sort(
        new Comparator<Candidate>() {
          @Override
          public int compare(Candidate left, Candidate right) {
            int byPre = Double.compare(right.pre, left.pre);
            if (byPre != 0) {
              return byPre;
            }
            return left.key.compareTo(right.key);
          }
        });

    // 罕见网关全部进入 deep，可以超过 maxSignals；其余名额按预评分从高到低补足。
    long gatewayCount = candidates.stream().filter(c -> c.gateway).count();
    int remaining = Math.max(0, config.maxSignals - (int) gatewayCount);
    List<Map<String, Object>> signals = new ArrayList<>();
    List<Map<String, Object>> shallow = new ArrayList<>();
    for (Candidate candidate : candidates) {
      boolean deep = candidate.gateway || remaining > 0;
      if (!candidate.gateway && remaining > 0) {
        remaining--;
      }
      if (!deep) {
        shallow.add(shallowRow(candidate));
        continue;
      }
      signals.add(deepSignal(family, candidate, targetDay, baselineDays, baseline.parentPaths));
    }
    addNovelHosts(signals, targetDay, baseline.hosts);
    if (family.equals("tracing_sas_linux")) {
      signals.addAll(behavior(targetDay, baselineDays, true));
    }
    sort(signals);
    int cap = Math.max(config.maxSignals + 30, 200);
    return new Result(
        new ArrayList<>(signals.subList(0, Math.min(cap, signals.size()))),
        shallow,
        obj(
            "target_hosts",
            targetDay.hosts.size(),
            "baseline_hosts",
            baseline.hosts.size(),
            "candidate_triples",
            candidates.size(),
            "signals",
            signals.size(),
            "fleet_mode",
            config.get("fleet.mode", "legacy_snapshot"),
            "history_compare",
            true));
  }

  /**
   * 不和基线比较。三元组只在命令结构规则命中时留下；Linux 再叠加行为模式。新主机、次数突增、模板差集都不产出。
   */
  private static Result sasRules(String family, DayBucket targetDay, MiningConfig config) {
    List<Map<String, Object>> signals = new ArrayList<>();
    for (DayBucket.Aggregate aggregate : targetDay.triples.values()) {
      Map<String, Object> row = ruleSignal(family, aggregate);
      if (row != null) {
        signals.add(row);
      }
    }
    if (family.equals("tracing_sas_linux")) {
      signals.addAll(behavior(targetDay, Collections.<DayBucket>emptyList(), false));
    }
    sort(signals);
    int cap = Math.max(config.maxSignals + 30, 200);
    List<Map<String, Object>> kept =
        new ArrayList<>(signals.subList(0, Math.min(cap, signals.size())));
    return new Result(
        kept,
        Collections.<Map<String, Object>>emptyList(),
        obj(
            "target_hosts",
            targetDay.hosts.size(),
            "candidate_triples",
            targetDay.triples.size(),
            "signals",
            kept.size(),
            "history_compare",
            false));
  }

  /** 全量紧凑事件用于结构规则；展示样本仍有界。 */
  private static Map<String, Object> ruleSignal(String family, DayBucket.Aggregate aggregate) {
    List<Event> samples = aggregate.orderedScoringEvents();
    if (samples.isEmpty()) {
      return null;
    }
    Event sample = samples.get(0);
    String proc = sample.s("proc_name");
    Map<String, Integer> templates = aggregate.commandTemplates;
    Map<String, Integer> chains = aggregate.chainSignatures;
    Scoring.Adjustment adjustment =
        Scoring.adjust(
            family,
            0,
            proc,
            samples,
            templates,
            chains,
            Collections.<String>emptySet(),
            0,
            false);
    Object reasons = adjustment.breakdown().get("structural_reasons");
    if (!(reasons instanceof List) || ((List<?>) reasons).isEmpty()) {
      return null;
    }
    double finalScore = adjustment.score();
    if (((List<?>) reasons).contains("rare_gateway_proc")) {
      finalScore = round(finalScore + 85);
      adjustment.breakdown().put("gateway_priority_boost", 85.0);
    }
    Map<String, Object> row =
        signal("rule_match", sample.s("host_ip"), sample.s("username"), proc, finalScore);
    row.put("score_breakdown", adjustment.breakdown());
    row.put("target_count", aggregate.count);
    List<Event> ordered = new ArrayList<>(aggregate.samples);
    ordered.sort(
        new Comparator<Event>() {
          @Override
          public int compare(Event left, Event right) {
            return Integer.compare(
                left.s("cmdline").trim().isEmpty() ? 1 : 0, right.s("cmdline").trim().isEmpty() ? 1 : 0);
          }
        });
    row.put("samples", sample(ordered, 2));
    return row;
  }

  private static Baseline baseline(String family, List<DayBucket> baselineDays) {
    Set<String> hosts = new HashSet<>();
    Map<String, Long> tripleCounts = new HashMap<>();
    Map<String, Set<String>> procHosts = new HashMap<>();
    Set<String> parentPaths = new HashSet<>();
    for (DayBucket day : baselineDays) {
      hosts.addAll(day.hosts.keySet());
      day.procHosts.forEach(
          (proc, hostSet) -> procHosts.computeIfAbsent(proc, key -> new HashSet<>()).addAll(hostSet));
      for (Map.Entry<String, DayBucket.Aggregate> entry : day.triples.entrySet()) {
        tripleCounts.merge(entry.getKey(), entry.getValue().count, Long::sum);
        Event sample = entry.getValue().samples.get(0);
        procHosts.computeIfAbsent(sample.s("proc_name"), key -> new HashSet<>()).add(sample.s("host_ip"));
      }
      parentPaths.addAll(day.fetchParentPaths);
    }
    return new Baseline(hosts, tripleCounts, procHosts, parentPaths);
  }

  /** 基线侧保留父命令里的全部路径；评分侧只拿第一段的第一个路径做新颖比较。 */
  static void rememberParentPaths(String family, Event event, Set<String> parentPaths) {
    List<String> parents = Templates.parents(event.s("cmd_chain"));
    if (parents.isEmpty()) {
      return;
    }
    String parent = parents.get(0);
    if (family.equals("tracing_sas_win")) {
      for (String token : parent.split("\\s+")) {
        String cleaned = token.replaceAll("^[\"']+|[\"']+$", "");
        if (cleaned.contains(":") && cleaned.contains("\\")) {
          parentPaths.add(cleaned);
        }
      }
      return;
    }
    String invokedPath = invoked(parent);
    if (!invokedPath.isEmpty()) {
      parentPaths.add(invokedPath);
    }
    for (String token : parent.split("\\s+")) {
      if (token.startsWith("/") && token.length() > 4) {
        parentPaths.add(token);
      }
    }
  }

  private static List<Candidate> candidates(
      String family, LocalDate target, DayBucket targetDay, Baseline baseline, MiningConfig config) {
    List<Candidate> candidates = new ArrayList<>();
    for (Map.Entry<String, DayBucket.Aggregate> entry : targetDay.triples.entrySet()) {
      DayBucket.Aggregate aggregate = entry.getValue();
      if (aggregate.count < config.minCount) {
        continue;
      }
      Event sample = aggregate.samples.get(0);
      String proc = sample.s("proc_name");
      long baselineCount = baseline.tripleCounts.getOrDefault(entry.getKey(), 0L);
      int fleetHosts = fleetCount(family, target, proc, baseline.procHosts, config);
      // 过滤用未舍入的比值；对外保存两位小数。+1 是旧规则的平滑项。
      double rawRatio = baselineCount > 0 ? aggregate.count / (baselineCount + 1.0) : aggregate.count;
      double ratio = round(rawRatio);
      // 候选阶段用进程全名精确匹配。Windows 去掉扩展名的比较只出现在结构加分里。
      boolean gateway =
          RuleData.has(family, "RARE_GATEWAY_PROCS", proc.toLowerCase(Locale.ROOT)) && fleetHosts <= 25;
      if (baselineCount > 0 && rawRatio < 2 && !gateway) {
        continue;
      }
      candidates.add(
          new Candidate(
              entry.getKey(),
              sample.s("host_ip"),
              sample.s("username"),
              proc,
              aggregate.count,
              baselineCount,
              !baseline.hosts.contains(sample.s("host_ip")),
              fleetHosts,
              ratio,
              pre(baselineCount == 0, aggregate.count, baselineCount, ratio, fleetHosts, proc, family),
              gateway));
    }
    return candidates;
  }

  private static Map<String, Object> shallowRow(Candidate candidate) {
    return obj(
        "host_ip",
        candidate.host,
        "username",
        candidate.user,
        "proc_name",
        candidate.proc,
        "target_count",
        candidate.target,
        "baseline_count_max",
        candidate.baseline,
        "spike_ratio",
        candidate.ratio,
        "pre_score",
        candidate.pre);
  }

  private static Map<String, Object> deepSignal(
      String family,
      Candidate candidate,
      DayBucket targetDay,
      List<DayBucket> baselineDays,
      Set<String> parentPaths) {
    DayBucket.Aggregate target = targetDay.triples.get(candidate.key);
    List<Event> samples = target.orderedScoringEvents();
    Set<String> baselineTemplates = new HashSet<>();
    Set<String> baselineChains = new HashSet<>();
    for (DayBucket day : baselineDays) {
      DayBucket.Aggregate aggregate = day.triples.get(candidate.key);
      if (aggregate == null) {
        continue;
      }
      baselineTemplates.addAll(aggregate.commandTemplates.keySet());
      baselineChains.addAll(aggregate.chainSignatures.keySet());
    }
    Map<String, Integer> templates = new TreeMap<>(target.commandTemplates);
    Map<String, Integer> chains = new TreeMap<>(target.chainSignatures);
    templates.keySet().removeAll(baselineTemplates);
    chains.keySet().removeAll(baselineChains);
    double baseScore =
        score(
            candidate.novelHost,
            candidate.baseline == 0,
            candidate.target,
            candidate.baseline,
            candidate.ratio,
            candidate.fleet,
            templates.size(),
            chains.size());
    Scoring.Adjustment adjustment =
        adjust(family, baseScore, candidate.proc, samples, templates, chains, parentPaths, candidate.fleet);
    double finalScore = adjustment.score();
    if (candidate.gateway) {
      finalScore = round(finalScore + 85);
      adjustment.breakdown().put("gateway_priority_boost", 85.0);
    }
    Map<String, Object> row =
        signal(
            signalType(candidate.baseline == 0, !templates.isEmpty()),
            candidate.host,
            candidate.user,
            candidate.proc,
            finalScore);
    row.put("score_breakdown", adjustment.breakdown());
    row.put("target_count", candidate.target);
    row.put("baseline_count_max", candidate.baseline);
    row.put("spike_ratio", candidate.ratio);
    row.put("fleet_proc_hosts_in_baseline", candidate.fleet);
    row.put("novel_templates", features(templates, "template", 8));
    row.put("novel_chain_signatures", features(chains, "chain_sig", 5));
    List<Event> ordered = new ArrayList<>(target.samples);
    // 空命令靠后，两条样本优先留有命令行的。排序稳定，同组仍按事件时间。
    ordered.sort(
        new Comparator<Event>() {
          @Override
          public int compare(Event left, Event right) {
            return Integer.compare(
                left.s("cmdline").trim().isEmpty() ? 1 : 0, right.s("cmdline").trim().isEmpty() ? 1 : 0);
          }
        });
    row.put("samples", sample(ordered, 2));
    return row;
  }

  private static String signalType(boolean novelTriple, boolean novelTemplate) {
    if (novelTriple) {
      return novelTemplate ? "novel_triple_and_template" : "novel_triple";
    }
    return novelTemplate ? "novel_template" : "history_diff";
  }

  /** 新主机独立于三元组深挖，按当天事件数取前 25。 */
  private static void addNovelHosts(
      List<Map<String, Object>> signals, DayBucket targetDay, Set<String> baselineHosts) {
    targetDay.hosts.entrySet().stream()
        .filter(entry -> !baselineHosts.contains(entry.getKey()))
        .sorted(
            new Comparator<Map.Entry<String, Long>>() {
              @Override
              public int compare(Map.Entry<String, Long> left, Map.Entry<String, Long> right) {
                int byCount = Long.compare(right.getValue(), left.getValue());
                if (byCount != 0) {
                  return byCount;
                }
                return left.getKey().compareTo(right.getKey());
              }
            })
        .limit(25)
        .forEach(
            entry -> {
              Map<String, Object> row =
                  signal("novel_host", entry.getKey(), "", "", 35 + Math.min(entry.getValue() / 20.0, 25));
              row.put("events_on_day", entry.getValue());
              row.put(
                  "first_logs",
                  targetDay.firstHost.get(entry.getKey()).stream().map(event -> event.fields).collect(Collectors.toList()));
              signals.add(row);
            });
  }

  /**
   * exact 用基线桶里的主机去重。legacy 读取冻结的 ES 快照：缺少整天快照是错误，快照里没有该进程则按 0。
   */
  @SuppressWarnings("unchecked")
  private static int fleetCount(
      String family,
      LocalDate target,
      String proc,
      Map<String, Set<String>> exactHosts,
      MiningConfig config) {
    if (config.get("fleet.mode", "legacy_snapshot").equals("exact")) {
      return exactHosts.getOrDefault(proc, Collections.<String>emptySet()).size();
    }
    Object familySnapshots = config.fleetSnapshots.get(family);
    if (!(familySnapshots instanceof Map)) {
      throw new IllegalStateException("Missing legacy fleet snapshot for " + family + " " + target);
    }
    Map<?, ?> byDay = (Map<?, ?>) familySnapshots;
    if (!(byDay.get(target.toString()) instanceof Map)) {
      throw new IllegalStateException("Missing legacy fleet snapshot for " + family + " " + target);
    }
    Map<?, ?> counts = (Map<?, ?>) byDay.get(target.toString());
    Object count = counts.get(proc);
    return count == null ? 0 : ((Number) count).intValue();
  }

  private static List<Map<String, Object>> features(Map<String, Integer> counts, String key, int limit) {
    return counts.entrySet().stream()
        .sorted(
            new Comparator<Map.Entry<String, Integer>>() {
              @Override
              public int compare(Map.Entry<String, Integer> left, Map.Entry<String, Integer> right) {
                int byCount = Integer.compare(right.getValue(), left.getValue());
                if (byCount != 0) {
                  return byCount;
                }
                return left.getKey().compareTo(right.getKey());
              }
            })
        .limit(limit)
        .map(entry -> obj(key, entry.getKey(), "count", entry.getValue()))
        .collect(Collectors.toList());
  }

  /** Behavior thresholds and baseline comparisons use full per-host counts. */
  private static List<Map<String, Object>> behavior(
      DayBucket targetDay, List<DayBucket> baselineDays, boolean historyCompare) {
    targetDay.requireCompatibleBehaviorState();
    for (DayBucket baseline : baselineDays) baseline.requireCompatibleBehaviorState();
    List<Map<String, Object>> signals = new ArrayList<>();
    for (Map<String, Object> pattern : Behavior.patterns()) {
      String label = pattern.get("label").toString();
      String postFilter = pattern.getOrDefault("post_filter", "").toString();
      boolean lateral = postFilter.equals("_lateral_novel_dst");
      if (lateral) {
        if (historyCompare) signals.addAll(lateralSignals(pattern, targetDay, baselineDays));
        continue;
      }
      if (!postFilter.isEmpty() && !lateral && !postFilter.equals("_dns_tunnel_heuristic")) {
        throw new IllegalArgumentException("Unsupported behavior post-filter: " + postFilter);
      }
      int minimum = ((Number) pattern.getOrDefault("min_count_threshold", 1)).intValue();
      Map<String, Map<String, DayBucket.Aggregate>> hosts =
          targetDay.behaviorHosts.getOrDefault(label, Collections.emptyMap());
      for (Map.Entry<String, Map<String, DayBucket.Aggregate>> host : hosts.entrySet()) {
        DayBucket.Aggregate hits = behaviorCount(host.getValue(), Collections.<String>emptySet());
        if (hits.count < minimum || hits.samples.isEmpty()) continue;
        long baselineMax = 0;
        if (historyCompare) {
          for (DayBucket day : baselineDays.subList(0, Math.min(3, baselineDays.size()))) {
            Map<String, DayBucket.Aggregate> groups = day.behaviorHosts
                .getOrDefault(label, Collections.emptyMap()).getOrDefault(host.getKey(), Collections.emptyMap());
            baselineMax = Math.max(baselineMax,
                behaviorCount(groups, Collections.<String>emptySet()).count);
          }
        }
        double ratio = historyCompare ? hits.count / (baselineMax + 1.0) : hits.count;
        if (historyCompare && baselineMax > 0 && ratio < 2) continue;
        signals.add(behaviorSignal(pattern, label, host.getKey(), hits.samples, hits.count,
            baselineMax, ratio, historyCompare));
      }
    }
    return signals;
  }

  private static DayBucket.Aggregate behaviorCount(
      Map<String, DayBucket.Aggregate> groups, Set<String> excludedDestinations) {
    DayBucket.Aggregate result = new DayBucket.Aggregate();
    for (Map.Entry<String, DayBucket.Aggregate> group : groups.entrySet()) {
      if (!group.getKey().isEmpty() && excludedDestinations.contains(group.getKey())) continue;
      result.count += group.getValue().count;
      for (Event event : group.getValue().samples) DayBucket.keep(result.samples, event, 30, false);
    }
    return result;
  }

  /** Compare destination sets within the same source host and user, using all stored events. */
  private static List<Map<String, Object>> lateralSignals(
      Map<String, Object> pattern, DayBucket target, List<DayBucket> baselineDays) {
    String label = pattern.get("label").toString();
    List<Map<String, Object>> signals = new ArrayList<>();
    Map<String, Map<String, DayBucket.Aggregate>> hosts =
        target.behaviorHosts.getOrDefault(label, Collections.emptyMap());
    for (Map.Entry<String, Map<String, DayBucket.Aggregate>> host : hosts.entrySet()) {
      Map<String, Map<String, DayBucket.Aggregate>> actors = new TreeMap<>();
      for (Map.Entry<String, DayBucket.Aggregate> group : host.getValue().entrySet()) {
        if (group.getValue().samples.isEmpty()) continue;
        String user = group.getValue().samples.get(0).s("username");
        actors.computeIfAbsent(user, ignored -> new TreeMap<>()).put(group.getKey(), group.getValue());
      }
      Map<String, Set<String>> knownByActor = new HashMap<>();
      if (!host.getKey().isEmpty()) {
        for (DayBucket day : baselineDays.subList(0, Math.min(3, baselineDays.size()))) {
          Map<String, DayBucket.Aggregate> groups = day.behaviorHosts
              .getOrDefault(label, Collections.emptyMap()).getOrDefault(host.getKey(), Collections.emptyMap());
          for (Map.Entry<String, DayBucket.Aggregate> group : groups.entrySet()) {
            if (group.getValue().samples.isEmpty()) continue;
            String user = group.getValue().samples.get(0).s("username");
            if (!user.isEmpty()) knownByActor.computeIfAbsent(user, ignored -> new HashSet<>()).add(group.getKey());
          }
        }
      }
      for (Map.Entry<String, Map<String, DayBucket.Aggregate>> actor : actors.entrySet()) {
        Set<String> known = knownByActor.getOrDefault(actor.getKey(), Collections.emptySet());
        DayBucket.Aggregate hits = behaviorCount(actor.getValue(), known);
        int minimum = ((Number) pattern.getOrDefault("min_count_threshold", 1)).intValue();
        if (hits.count >= minimum && !hits.samples.isEmpty()) {
          signals.add(behaviorSignal(pattern, label, host.getKey(), hits.samples, hits.count, 0, hits.count, true));
        }
      }
    }
    return signals;
  }

  private static Map<String, Object> behaviorSignal(
      Map<String, Object> pattern,
      String label,
      String host,
      List<Event> events,
      long hitCount,
      long baselineMax,
      double ratio,
      boolean historyCompare) {
    String severity = pattern.get("severity").toString();
    double score = severity.equals("critical") ? 85 : severity.equals("high") ? 70 : 55;
    if (historyCompare) {
      score += (baselineMax == 0 ? 15 : 0) + (ratio >= 5 ? 10 : ratio >= 3 ? 5 : 0);
    }
    score += Math.min(hitCount * 2, 15);
    Map<String, Object> row =
        signal(
            "behavior_pattern",
            host,
            events.get(0).s("username"),
            events.get(0).s("proc_name"),
            Math.min(score, 120));
    row.put("behavior_label", label);
    row.put("description", pattern.get("description"));
    row.put("severity", severity);
    row.put("hit_count", hitCount);
    if (historyCompare) {
      row.put("baseline_max_count", baselineMax);
      row.put("is_novel", baselineMax == 0);
      row.put("spike_ratio", round(ratio));
    }
    row.put("samples", sample(events, 5));
    return row;
  }

  /** 同分时的稳定身份。不含分数，同一实体重新打分后 id 不变。 */
  public static String identity(Map<String, Object> signal) {
    return Json.write(
        J8.list(
            signal.getOrDefault("signal_type", ""),
            signal.getOrDefault("host_ip", ""),
            signal.getOrDefault("username", ""),
            signal.getOrDefault("proc_name", ""),
            signal.getOrDefault("behavior_label", ""),
            signal.getOrDefault("entity_key", "")));
  }

  public static void sort(List<Map<String, Object>> signals) {
    signals.sort(
        new Comparator<Map<String, Object>>() {
          @Override
          public int compare(Map<String, Object> left, Map<String, Object> right) {
            int byScore =
                Double.compare(
                    ((Number) right.get("score")).doubleValue(), ((Number) left.get("score")).doubleValue());
            if (byScore != 0) {
              return byScore;
            }
            return identity(left).compareTo(identity(right));
          }
        });
  }
}
