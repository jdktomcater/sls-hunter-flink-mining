package com.sls.hunter.flink;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Fixed-window rule evaluation. History consists of explicitly complete same-time slots. */
final class WindowMining {
  private WindowMining() {}

  static long windowStart(long eventTime, long windowSizeMillis) {
    return Math.floorDiv(eventTime, windowSizeMillis) * windowSizeMillis;
  }

  static MiningEngine.Result evaluate(String family, DayBucket bucket, LocalDate day, MiningConfig config) {
    Map<String, DayBucket> days = new HashMap<>();
    days.put(day.toString(), bucket);
    return MiningEngine.evaluate(family, day, days, config);
  }

  /** Evaluate every event, not just the first 30 samples. History-dependent post-filters are excluded. */
  @SuppressWarnings("unchecked")
  static void recordHighRisk(DayBucket bucket, Event event) {
    if (!event.family.equals("tracing_nginx_hertzbeat")) {
      List<Event> one = Collections.singletonList(event);
      Scoring.Adjustment a = Scoring.adjust(event.family, 0, event.s("proc_name"), one,
          Templates.commands(one), Templates.chains(one), Collections.<String>emptySet(), 0, false);
      Object reasons = a.breakdown().get("structural_reasons");
      Set<String> highRisk = J8.set("pipe_to_interpreter", "encoded_powershell", "lolbin_suspicious_target");
      if (reasons instanceof List && ((List<?>) reasons).stream().anyMatch(highRisk::contains)) {
        String key = Json.write(J8.list(event.s("host_ip"), event.s("username"), event.s("proc_name")));
        DayBucket.Aggregate aggregate = bucket.windowStructural.computeIfAbsent(key, ignored -> new DayBucket.Aggregate());
        aggregate.add(event, 5);
        for (Object reason : (List<?>) reasons) aggregate.structuralReasons.add(String.valueOf(reason));
      }
    }
    if (!event.family.equals("tracing_sas_linux")) return;
    for (Map<String, Object> pattern : Behavior.patterns()) {
      String severity = String.valueOf(pattern.get("severity"));
      if (!(severity.equals("critical") || severity.equals("high"))
          || !String.valueOf(pattern.getOrDefault("post_filter", "")).isEmpty()
          || !Behavior.matches(pattern.get("query"), event)) continue;
      if (!Behavior.included(pattern, event)) continue;
      String key = Json.write(J8.list(pattern.get("label"), event.s("host_ip")));
      bucket.windowHighRisk.computeIfAbsent(key, ignored -> new DayBucket.Aggregate()).add(event, 5);
    }
  }

  static MiningEngine.Result evaluateChannels(String family, DayBucket target, List<DayBucket> baseline,
      boolean ready, MiningConfig config) {
    target.requireCompatibleBehaviorState();
    for (DayBucket bucket : baseline) bucket.requireCompatibleBehaviorState();
    List<Map<String, Object>> signals = new ArrayList<>();
    for (DayBucket.Aggregate a : target.windowStructural.values()) {
      if (a.samples.isEmpty()) continue;
      Event first = a.samples.get(0);
      signals.add(Json.obj("signal_type", "high_risk_structure", "channel", "high_risk", "host_ip", first.s("host_ip"),
          "username", first.s("username"), "proc_name", first.s("proc_name"), "score", 70,
          "hit_count", a.count, "structural_reasons", new ArrayList<>(a.structuralReasons), "samples", a.samples));
    }
    for (Map<String, Object> pattern : Behavior.patterns()) {
      String label = String.valueOf(pattern.get("label"));
      for (Map.Entry<String, DayBucket.Aggregate> entry : target.windowHighRisk.entrySet()) {
        DayBucket.Aggregate a = entry.getValue();
        if (a.samples.isEmpty() || !entry.getKey().equals(Json.write(J8.list(label, a.samples.get(0).s("host_ip"))))) continue;
        int minimum = ((Number) pattern.getOrDefault("min_count_threshold", 1)).intValue();
        if (a.count < minimum || a.samples.isEmpty()) continue;
        Event first = a.samples.get(0);
        Map<String, Object> row = Json.obj("signal_type", "behavior_pattern", "channel", "high_risk",
            "host_ip", first.s("host_ip"), "username", first.s("username"), "proc_name", first.s("proc_name"),
            "behavior_label", label, "description", pattern.get("description"), "severity", pattern.get("severity"),
            "score", "critical".equals(pattern.get("severity")) ? 85 : 70,
            "hit_count", a.count, "samples", a.samples);
        signals.add(row);
      }
    }
    if (ready) {
      double threshold = Double.parseDouble(config.get("window.history.spike.ratio", "3.0"));
      double floor = Double.parseDouble(config.get("window.history.baseline.floor", "1.0"));
      if (family.equals("tracing_nginx_hertzbeat")) {
        for (Map.Entry<String, DayBucket.Aggregate> entry : target.uris.entrySet()) {
          long total = 0;
          for (DayBucket day : baseline) total += day.uris.getOrDefault(entry.getKey(), new DayBucket.Aggregate()).count;
          addHistory(signals, "uri", entry.getKey(), "", "", entry.getValue().count, total, baseline.size(), threshold, floor, config.minCount);
        }
        for (Map.Entry<String, DayBucket.Aggregate> entry : target.upstreams.entrySet()) {
          long total = 0;
          for (DayBucket day : baseline) total += day.upstreams.getOrDefault(entry.getKey(), new DayBucket.Aggregate()).count;
          if (!entry.getKey().isEmpty() && total == 0)
            addHistory(signals, "upstream", entry.getKey(), "", "", entry.getValue().count, total, baseline.size(), threshold, floor, config.minCount);
        }
      } else {
        for (Map.Entry<String, DayBucket.Aggregate> entry : target.triples.entrySet()) {
          DayBucket.Aggregate a = entry.getValue();
          if (a.samples.isEmpty()) continue;
          long total = 0;
          Set<String> known = new HashSet<>();
          for (DayBucket day : baseline) {
            DayBucket.Aggregate b = day.triples.get(entry.getKey());
            if (b != null) { total += b.count; known.addAll(b.commandTemplates.keySet()); }
          }
          Event first = a.samples.get(0);
          Set<String> novelTemplates = new HashSet<>(a.commandTemplates.keySet());
          novelTemplates.removeAll(known);
          addHistory(signals, "triple", first.s("host_ip"), first.s("username"), first.s("proc_name"), a.count,
              total, baseline.size(), threshold, floor, config.minCount);
          if (!novelTemplates.isEmpty() && a.count >= config.minCount) {
            List<String> sorted = new ArrayList<>(novelTemplates); Collections.sort(sorted);
            signals.add(Json.obj("signal_type", "window_novel_template", "channel", "history", "host_ip", first.s("host_ip"),
                "username", first.s("username"), "proc_name", first.s("proc_name"), "score", 45,
                "target_count", a.count, "baseline_count", total, "novel_templates", sorted, "samples", a.samples));
          }
        }
      }
    }
    MiningEngine.sort(signals);
    return new MiningEngine.Result(signals, Collections.<Map<String, Object>>emptyList(),
        Json.obj("history_compare", true, "baseline_ready", ready, "baseline_windows", baseline.size(), "target_events", target.events));
  }

  private static void addHistory(List<Map<String, Object>> out, String kind, String entity, String user, String proc,
      long count, long baseline, int n, double threshold, double floor, int minimum) {
    if (count < minimum) return;
    double average = baseline / (double) n;
    double ratio = count / Math.max(average, floor);
    if (baseline != 0 && ratio < threshold) return;
    String type = "window_" + (baseline == 0 ? "novel_" : "spike_") + kind;
    out.add(Json.obj("signal_type", type, "channel", "history", "host_ip", entity, "username", user,
        "proc_name", proc, "entity_key", entity, "score", baseline == 0 ? 50 : 40 + Math.min(ratio, 30),
        "target_count", count, "baseline_count", baseline, "baseline_average", average, "spike_ratio", Scoring.round(ratio)));
  }

  static List<Map<String, Object>> highScores(List<Map<String, Object>> signals, double minScore) {
    List<Map<String, Object>> kept = new ArrayList<>();
    for (Map<String, Object> signal : signals) {
      Object score = signal.get("score");
      if (score instanceof Number && ((Number) score).doubleValue() >= minScore) kept.add(signal);
    }
    return kept;
  }
}
