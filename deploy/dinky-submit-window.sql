-- Fixed event-time window job. Upload the same shaded JAR used by the day job.
-- conf/window-mining.properties is packaged at the jar root. Rebuild after changing it.
-- Use a consumer group and transaction prefix that are not shared with MiningJob.
-- Bind the matching Flink 1.18.x cluster running Java 8; runtime must supply Flink and RocksDB backend.
EXECUTE JAR WITH (
  'uri'='rs:/jar/sls-hunter-flink-mining/sls-hunter-flink-mining-1.0.0-SNAPSHOT.jar',
  'main-class'='com.sls.hunter.flink.WindowMiningJob',
  'args'='--config classpath:window-mining.properties'
);
