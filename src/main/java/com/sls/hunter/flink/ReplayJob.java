package com.sls.hunter.flink;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 有界规则回放。把文件里的 EVENT 按族和业务日装进内存桶，再对指定目标日评估一次。
 *
 * <p>不连接 Kafka，也不模拟定时器、迟到和封账；文件在评估前全部读完。
 */
public final class ReplayJob {
  private static final Logger LOG = LoggerFactory.getLogger(ReplayJob.class);

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      throw new IllegalArgumentException(
          "Usage: ReplayJob config.properties events.jsonl YYYY-MM-DD");
    }
    MiningConfig config = MiningConfig.load(new String[] {"--config", args[0]});
    LOG.info("Replay started: targetDay={}, baselineDays={}", args[2], config.baselineDays);
    Map<String, Map<String, DayBucket>> families = new TreeMap<String, Map<String, DayBucket>>();
    try (Stream<String> lines = Files.lines(Paths.get(args[1]))) {
      Iterator<String> it = lines.filter(line -> !line.trim().isEmpty()).iterator();
      while (it.hasNext()) {
        Event event = Event.parse(it.next());
        if (!event.kind.equals("EVENT")) {
          continue;
        }
        Map<String, DayBucket> days = families.get(event.family);
        if (days == null) {
          days = new TreeMap<String, DayBucket>();
          families.put(event.family, days);
        }
        DayBucket bucket = days.get(event.day);
        if (bucket == null) {
          bucket = new DayBucket();
          days.put(event.day, bucket);
        }
        bucket.add(event);
      }
    }
    for (Map.Entry<String, Map<String, DayBucket>> family : families.entrySet()) {
      MiningEngine.Result result =
          MiningEngine.evaluate(
              family.getKey(), LocalDate.parse(args[2]), family.getValue(), config);
      LOG.info(
          "Replay window evaluated: family={}, targetDay={}, retainedDays={}, signals={}, shallowCandidates={}",
          family.getKey(),
          args[2],
          family.getValue().size(),
          result.signals().size(),
          result.shallow().size());
      System.out.println(
          Json.write(
              Json.obj(
                  "index_family",
                  family.getKey(),
                  "target_day",
                  args[2],
                  "all_signals",
                  result.signals(),
                  "shallow_candidates",
                  result.shallow(),
                  "stats",
                  result.stats())));
    }
  }
}
