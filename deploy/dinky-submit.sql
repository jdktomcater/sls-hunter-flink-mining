-- Dinky 1.1 EXECUTE JAR. Upload the built artifact to this resource-center path.
-- conf/mining.properties is packaged at the jar root. Rebuild after changing it.
-- classpath: reads that entry from the jar. A plain path still reads the local filesystem.
-- fleet.snapshot.file accepts classpath: or a filesystem path.
-- Bind the matching Flink 1.18.x cluster running Java 8; runtime must supply Flink and RocksDB backend.
-- Logging uses the Flink process file:log4j.properties. This JAR does not ship a log config.
EXECUTE JAR WITH (
  'uri'='rs:/jar/sls-hunter-flink-mining/sls-hunter-flink-mining-1.0.0-SNAPSHOT.jar',
  'main-class'='com.sls.hunter.flink.MiningJob',
  'args'='--config classpath:mining.properties'
);
