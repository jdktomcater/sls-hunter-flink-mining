package com.sls.hunter.flink;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 固定时间窗口的打分入口。规则就是按日作业在 {@code history.compare=false} 时用的那套：命令结构加分和 Linux
 * 行为模式，不读前 N 天基线。空基线不能被当成「全部都是新增」。
 */
final class WindowMining {
  private WindowMining() {}

  /** 事件时间对齐到固定滚动窗口的起点，起点按 Unix 纪元取整。 */
  static long windowStart(long eventTime, long windowSizeMillis) {
    return Math.floorDiv(eventTime, windowSizeMillis) * windowSizeMillis;
  }

  static MiningEngine.Result evaluate(
      String family, DayBucket bucket, LocalDate day, MiningConfig config) {
    if (config.historyCompare) {
      throw new IllegalArgumentException(
          "Fixed-window scoring uses the structural and behavior rules with history.compare=false");
    }
    Map<String, DayBucket> days = new HashMap<>();
    days.put(day.toString(), bucket);
    return MiningEngine.evaluate(family, day, days, config);
  }

  /** 保留分数达到下限的信号，顺序仍是现有规则的从高到低。 */
  static List<Map<String, Object>> highScores(
      List<Map<String, Object>> signals, double minScore) {
    List<Map<String, Object>> kept = new ArrayList<>();
    for (Map<String, Object> signal : signals) {
      Object score = signal.get("score");
      if (score instanceof Number && ((Number) score).doubleValue() >= minScore) {
        kept.add(signal);
      }
    }
    return kept;
  }
}
