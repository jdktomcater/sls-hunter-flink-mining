package com.sls.hunter.flink;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 固定时间窗口作业。和 {@link MiningJob} 并行，使用自己的消费组和事务前缀，不改按日那张 {@code mining_window}。
 *
 * <p>窗口按事件时间滚动。历史模式分别计算高危行为和已封账的历史同时段异常；关闭历史模式时保留原规则。
 */
public final class WindowMiningJob {
  private static final Logger LOG = LoggerFactory.getLogger(WindowMiningJob.class);

  public static void main(String[] args) throws Exception {
    MiningConfig config = MiningConfig.load(args);
    validateWindowConfig(config);
    LOG.info(
        "Configuring fixed-window mining job: windowMillis={}, outOfOrdernessMillis={}, idleMillis={}, latenessMillis={}, minScore={}, historyCompare={}",
        config.windowSizeMillis,
        config.windowOutOfOrdernessMillis,
        config.windowIdleMillis,
        config.latenessMillis,
        config.windowMinScore,
        config.historyCompare);
    LOG.info(
        "Kafka routing: inputTopics={}, groupId={}, startingOffsets=earliest, outputTopic={}, dlqTopic={}, lateTopic={}, delivery=EXACTLY_ONCE, transactionTimeoutMs={}",
        config.required("kafka.input.topics"),
        config.required("kafka.group.id"),
        config.required("kafka.output.topic"),
        config.required("kafka.dlq.topic"),
        config.required("kafka.late.topic"),
        config.kafkaTransactionTimeoutMs);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(config.integer("parallelism", 4));
    env.setMaxParallelism(128);
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
    env.enableCheckpointing(
        config.integer("checkpoint.interval.ms", 5000), CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointStorage(config.required("checkpoint.storage"));
    env.getCheckpointConfig().setCheckpointTimeout(config.integer("checkpoint.timeout.ms", 120000));
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig()
        .enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    Properties kafka = config.kafkaProperties();
    kafka.setProperty("isolation.level", "read_committed");
    KafkaSource<MiningJob.SourcedRecord> source =
        KafkaSource.<MiningJob.SourcedRecord>builder()
            .setBootstrapServers(config.required("kafka.bootstrap.servers"))
            .setTopics(Arrays.asList(config.required("kafka.input.topics").split(",")))
            .setGroupId(config.required("kafka.group.id"))
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setProperties(kafka)
            .setDeserializer(new MiningJob.InputRecords())
            .build();
    SingleOutputStreamOperator<Event> parsed =
        env.fromSource(
                source, WatermarkStrategy.<MiningJob.SourcedRecord>noWatermarks(), "window-input")
            .uid("window-input-v1")
            .process(
                new MiningJob.Parse(
                    config.integer("max.future.skew.ms", 300000), config.topicFamilies()))
            .name("validate-window-input")
            .uid("validate-window-input-v1");
    WatermarkStrategy<Event> eventTime =
        WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                Duration.ofMillis(config.windowOutOfOrdernessMillis))
            .withTimestampAssigner(
                new SerializableTimestampAssigner<Event>() {
                  @Override
                  public long extractTimestamp(Event event, long recordTimestamp) {
                    return event.time;
                  }
                })
            .withIdleness(Duration.ofMillis(config.windowIdleMillis));
    SingleOutputStreamOperator<String> results =
        parsed
            .assignTimestampsAndWatermarks(eventTime)
            .name("fixed-window-watermarks")
            .uid("fixed-window-watermarks-v1")
            .keyBy(
                new KeySelector<Event, String>() {
                  @Override
                  public String getKey(Event event) {
                    return event.family;
                  }
                })
            .process(new WindowMiningFunction(config))
            .name("fixed-window-mining")
            .uid("fixed-window-mining-v1");
    results
        .sinkTo(MiningJob.sink(config, config.required("kafka.output.topic"), "window-fixed"))
        .uid("window-fixed-sink-v1");
    boolean writePostgres = config.jdbcEnabled();
    if (writePostgres) {
      String table = config.get("jdbc.window.table", "mining_window_fixed");
      LOG.info(
          "PostgreSQL fixed-window sink enabled: url={}, table={}, delivery=at-least-once upsert",
          PostgresSnapshots.redact(PostgresSnapshots.connectionUrl(config.required("jdbc.url"))),
          table);
      results
          .addSink(PostgresSnapshotSink.createFixed(config))
          .name("mining-window-fixed-jdbc")
          .uid("window-fixed-jdbc-v1");
    } else {
      LOG.info("PostgreSQL fixed-window sink disabled");
    }
    parsed
        .getSideOutput(MiningJob.DLQ)
        .sinkTo(MiningJob.sink(config, config.required("kafka.dlq.topic"), "window-dlq"))
        .uid("window-dlq-sink-v1");
    results
        .getSideOutput(WindowMiningFunction.LATE)
        .sinkTo(MiningJob.sink(config, config.required("kafka.late.topic"), "window-late"))
        .uid("window-late-sink-v1");
    LOG.info(
        "Submitting fixed-window mining job; Kafka delivery is EXACTLY_ONCE, PostgreSQL upsert is {}",
        writePostgres ? "enabled" : "disabled");
    env.execute(config.get("job.name", "sls-hunter-flink-mining-window"));
    LOG.info("Fixed-window mining job execution completed");
  }

  /** Historical comparison requires identical epoch-aligned slots on consecutive days. */
  static void validateWindowConfig(MiningConfig config) {
    if (config.historyCompare) {
      if (86_400_000L % config.windowSizeMillis != 0)
        throw new IllegalArgumentException("history comparison requires window.size.ms to divide a day");
      double ratio = Double.parseDouble(config.get("window.history.spike.ratio", "3.0"));
      double floor = Double.parseDouble(config.get("window.history.baseline.floor", "1.0"));
      if (!Double.isFinite(ratio) || ratio <= 0 || !Double.isFinite(floor) || floor <= 0)
        throw new IllegalArgumentException("window history ratio and floor must be finite and positive");
    }
  }
}
