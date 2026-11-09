package com.sls.hunter.flink;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** 作业参数。字段在构造时解析一次；fleet 快照可以在此之后按族覆盖。 */
public final class MiningConfig implements Serializable {
  public final Properties properties;
  public final int baselineDays;

  /** 为 false 时不读基线、不等封账和 fleet 快照，只跑行为模式和命令结构规则。 */
  public final boolean historyCompare;

  public final int minCount;
  public final int maxSignals;
  public final int retentionDays;
  public final long emitMillis;
  public final int flushMaxEvents;
  public final long flushMaxMillis;
  public final long checkpointMinPauseMillis;
  public final boolean checkpointUnaligned;
  public final long checkpointAlignedTimeoutMillis;
  public final long latenessMillis;

  /** 固定滚动窗口长度。只给 {@code WindowMiningJob} 使用，按日作业不读它。 */
  public final long windowSizeMillis;

  /** 事件时间乱序上限，用来生成固定窗口的 watermark。 */
  public final long windowOutOfOrdernessMillis;

  /** 输入分区空闲这么久后推进 watermark，避免一条静默分区把窗口一直关不上。 */
  public final long windowIdleMillis;

  /** 固定窗口快照里保留的最低分，含本数。0 表示现有规则打出来的信号都保留。 */
  public final double windowMinScore;

  public final long kafkaTransactionTimeoutMs;
  public final LocalDate firstTarget;
  public Map<String, Object> fleetSnapshots = Collections.emptyMap();

  public MiningConfig(Properties properties) {
    this.properties = properties;
    baselineDays = integer("baseline.days", 5);
    historyCompare = Boolean.parseBoolean(get("history.compare", "true"));
    minCount = integer("min.count", 2);
    maxSignals = integer("max.signals", 200);
    retentionDays = integer("retention.days", 14);
    flushMaxEvents = integer("flush.max.events", 2000);
    flushMaxMillis = parseLong("flush.max.ms", 100L);
    checkpointMinPauseMillis = parseLong("checkpoint.min.pause.ms", 10000L);
    checkpointAlignedTimeoutMillis = parseLong("checkpoint.aligned.timeout.ms", 10000L);
    String unaligned = get("checkpoint.unaligned.enabled", "true").trim();
    if (!unaligned.equals("true") && !unaligned.equals("false")) {
      throw new IllegalArgumentException("checkpoint.unaligned.enabled must be true or false");
    }
    checkpointUnaligned = Boolean.parseBoolean(unaligned);
    if (flushMaxEvents < 1
        || flushMaxMillis < 1
        || checkpointMinPauseMillis < 0
        || checkpointAlignedTimeoutMillis < 0) {
      throw new IllegalArgumentException("Invalid flush or checkpoint flow bounds");
    }
    emitMillis = integer("emit.interval.ms", 1000);
    latenessMillis = integer("allowed.lateness.ms", 600000);
    windowSizeMillis = parseLong("window.size.ms", 300000L);
    windowOutOfOrdernessMillis = parseLong("window.out.of.orderness.ms", 5000L);
    windowIdleMillis = parseLong("window.idle.ms", 60000L);
    windowMinScore = parseDouble("window.min.score", 0D);
    kafkaTransactionTimeoutMs = parseLong("kafka.transaction.timeout.ms", 180000L);
    firstTarget = resolveFirstTarget();
    if (!J8.set("exact", "legacy_snapshot").contains(get("fleet.mode", "legacy_snapshot"))) {
      throw new IllegalArgumentException("fleet.mode must be exact or legacy_snapshot");
    }
    // 保留窗口至少覆盖基线、目标日，再留出一天余量。
    if (baselineDays < 1
        || minCount < 1
        || maxSignals < 1
        || retentionDays < baselineDays + 2
        || emitMillis < 1
        || latenessMillis < 0
        || kafkaTransactionTimeoutMs
            <= Math.max((long) integer("checkpoint.interval.ms", 5000), checkpointMinPauseMillis)
                + integer("checkpoint.timeout.ms", 120000)) {
      throw new IllegalArgumentException(
          "Invalid mining bounds, or kafka.transaction.timeout.ms does not exceed max(checkpoint.interval.ms, checkpoint.min.pause.ms) + checkpoint.timeout.ms");
    }
    if (windowSizeMillis < 1000
        || windowOutOfOrdernessMillis < 0
        || windowIdleMillis < 1000
        || windowMinScore < 0
        || Double.isNaN(windowMinScore)
        || Double.isInfinite(windowMinScore)) {
      throw new IllegalArgumentException(
          "window.size.ms must be at least 1000, window.idle.ms at least 1000, and window.min.score must be zero or greater");
    }
  }

  private double parseDouble(String key, double defaultValue) {
    try {
      return Double.parseDouble(get(key, Double.toString(defaultValue)).trim());
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(key + " must be a number");
    }
  }

  private long parseLong(String key, long defaultValue) {
    try {
      return Long.parseLong(get(key, Long.toString(defaultValue)).trim());
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(key + " must be a number");
    }
  }

  /** 未配置时取启动当日减 3 天，时区固定 +08:00。显式日期优先。该值在进程启动时确定，运行中不随墙钟前移。 */
  private LocalDate resolveFirstTarget() {
    String configured = get("target.start.date", "").trim();
    if (configured.isEmpty()) {
      return LocalDate.now(Event.ZONE).minusDays(3);
    }
    return LocalDate.parse(configured);
  }

  public int integer(String key, int defaultValue) {
    return Integer.parseInt(properties.getProperty(key, Integer.toString(defaultValue)));
  }

  public String get(String key, String defaultValue) {
    return properties.getProperty(key, defaultValue);
  }

  /** {@code true} 或 {@code false}。其它拼写直接失败，避免把数据库 sink 悄悄关掉。 */
  public boolean jdbcEnabled() {
    String value = get("jdbc.enabled", "false").trim();
    if (value.equals("true")) {
      return true;
    }
    if (value.equals("false")) {
      return false;
    }
    throw new IllegalArgumentException("jdbc.enabled must be true or false");
  }

  public String required(String key) {
    String value = properties.getProperty(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing " + key);
    }
    return value;
  }

  /**
   * 参数形式为 {@code --config classpath:mining.properties} 或 {@code --config /path/mining.properties}。
   * {@code classpath:} 从 Jar 内读取，其余按本地文件读取。legacy 模式的 {@code fleet.snapshot.file} 使用同一规则。
   */
  public static MiningConfig load(String[] args) throws IOException {
    if (args.length != 2 || !args[0].equals("--config")) {
      throw new IllegalArgumentException(
          "Usage: --config classpath:mining.properties | /path/mining.properties");
    }
    Properties properties = new Properties();
    try (Reader reader = reader(args[1])) {
      properties.load(reader);
    }
    MiningConfig config = new MiningConfig(properties);
    String snapshotFile = config.get("fleet.snapshot.file", "");
    if (config.get("fleet.mode", "legacy_snapshot").equals("legacy_snapshot")
        && !snapshotFile.trim().isEmpty()) {
      config.fleetSnapshots = Json.map(readString(snapshotFile));
    }
    return config;
  }

  /** {@code classpath:} 开头的位置从当前 Jar 读取，否则当作本地文件。 */
  private static Reader reader(String location) throws IOException {
    return new InputStreamReader(open(location), StandardCharsets.UTF_8);
  }

  private static String readString(String location) throws IOException {
    try (InputStream input = open(location)) {
      return new String(J8.readAll(input), StandardCharsets.UTF_8);
    }
  }

  private static InputStream open(String location) throws IOException {
    String prefix = "classpath:";
    if (location.startsWith(prefix)) {
      String name = location.substring(prefix.length());
      if (name.startsWith("/")) {
        name = name.substring(1);
      }
      if (name.trim().isEmpty()) {
        throw new IllegalArgumentException("Missing classpath resource name");
      }
      InputStream input = MiningConfig.class.getClassLoader().getResourceAsStream(name);
      if (input == null) {
        throw new IllegalArgumentException("Missing classpath resource " + name);
      }
      return input;
    }
    return Files.newInputStream(Paths.get(location));
  }

  /**
   * {@code kafka.topic.family.<topic>=tracing_sas_linux} 把原始 topic 指到族。未配置的 topic 仍按名字识别
   * sas-linux、sas-win、jumper、nginx、hertzbeat。
   */
  public Map<String, String> topicFamilies() {
    Map<String, String> families = new LinkedHashMap<String, String>();
    String prefix = "kafka.topic.family.";
    properties.forEach(
        (key, value) -> {
          String name = key.toString();
          if (name.startsWith(prefix)) {
            families.put(name.substring(prefix.length()), value.toString().trim());
          }
        });
    return families;
  }

  /** 只转发 {@code kafka.property.} 前缀之后的项，前缀本身不进入客户端配置。 */
  public Properties kafkaProperties() {
    Properties kafka = new Properties();
    properties.forEach(
        (key, value) -> {
          if (key.toString().startsWith("kafka.property.")) {
            kafka.put(key.toString().substring("kafka.property.".length()), value);
          }
        });
    return kafka;
  }
}
