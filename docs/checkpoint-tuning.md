# MiningJob checkpoint 超时排查与配置

截图中成功 checkpoint 用时约 4–7 分钟，失败项在 600000 ms 超时；成功项状态量约 20–30 MB。仅凭总量与总耗时不能确定根因，增量 RocksDB checkpoint 的大小也不是全部存量状态大小。不要据此认定 HDFS 慢或直接继续放宽 timeout。

## 本次调整

- 每条 Kafka 输入、接纳、缓冲、折桶和发送日志降到 DEBUG；保留批次、评估和异常的汇总日志。
- 仅当 family 的最高业务日推进时扫描保留日桶；日桶、UID、maxParallelism、Kafka EXACTLY_ONCE 与状态名称不变。
- 定时器每次最多折入 `flush.max.events=2000` 条，处理循环达到 `flush.max.ms=100` 后也让出线程。已处理日桶落入托管状态后，才删除对应 pending 项。未完成项和续跑定时器一同随 checkpoint 恢复。
- 积压未清完时，在当前处理时间之后 1 ms 续跑；积压清空后再发布快照。持续追不上输入时，发布会延后，需要观察消费 lag，不能靠分批参数解决容量不足。
- 当轮已经更新的日桶直接用于计算，避免再次反序列化。
- JDBC sink 取消与上游的算子链绑定，减小同步 JDBC 调用占用 mining 线程的影响。新增 `Slow JDBC checkpoint flush` 日志；JDBC 本身仍会在 checkpoint 时同步刷写，Kafka 和 JDBC 仍非同一事务。

## checkpoint 参数

仓库中的 `mining.properties` 与示例使用：

```properties
checkpoint.interval.ms=30000
checkpoint.min.pause.ms=10000
checkpoint.unaligned.enabled=true
checkpoint.aligned.timeout.ms=10000
flush.max.events=2000
flush.max.ms=100
```

仅 `MiningJob` 应用新增的 checkpoint flow 配置；`WindowMiningJob` 的拓扑和 checkpoint 配置未修改。默认先尝试对齐，等待超过 10 秒后可转为非对齐；最多同时运行一个 checkpoint。非对齐会将网络中的数据纳入快照，可能增加 checkpoint 大小。需要回退时设置 `checkpoint.unaligned.enabled=false`。

Flink 1.18 文档说明，非对齐能够缓解反压导致的 barrier 等待，但不能打断长时间运行的记录处理/定时器：[Checkpointing under backpressure](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/checkpointing_under_backpressure/)。

`flush.max.ms` 只是折桶循环的协作让出预算，不是整个回调的硬超时。读取/写入单个巨大 JSON 日桶、加载保留日桶和规则计算仍同步执行。若这些阶段仍需分钟级，下一步应依据 profiling 将去重身份、聚合项从整日 JSON 拆成细粒度状态，并制定状态迁移方案；这次没有改变旧状态格式。

Kafka 事务超时的配置检查包含 `max(checkpoint.interval.ms, checkpoint.min.pause.ms) + checkpoint.timeout.ms`，还应为实际重启、恢复、提交延迟和失败的 checkpoint 预留余量，并与 broker 的 `transaction.max.timeout.ms` 匹配。这只是配置下限，不代表连续失败时事务一定不会超时。此次没有延长 checkpoint timeout 或 Kafka transaction timeout。

## 上线验证

1. 用匹配本次代码的 Jar 和外部配置重新提交；保留原 UID、事务前缀及 maxParallelism，从 savepoint 恢复。先确认恢复与 Kafka 事务初始化成功。
2. 连续观察至少 10 次 checkpoint，在 Flink UI 展开每个算子的每个 subtask，记录 Start Delay、Alignment Duration、Sync/Async Duration、持久化大小；与相同输入负载的旧版本比较。不能只看列表总时长。
3. 高 Start Delay/Alignment 加上反压，先检查下游吞吐和非对齐是否生效；高同步耗时对照 `Mining flush`、`Window evaluated` 和 `Slow JDBC checkpoint flush` 日志；高异步耗时检查 HDFS 写入、NameNode 延迟和 TaskManager 磁盘。
4. 同时检查 Kafka lag、CPU、GC、TaskManager busy/backpressured 时间及结果可见延迟。当前按 family 分区，一个热点 family 只能在一个子任务计算，单纯增大 parallelism 无法拆开它。
5. 若 JDBC 慢刷写，检查 PostgreSQL 锁、慢 SQL、网络及连接参数。现有 URL 只默认限制建连超时，不限制查询/读响应时间；新增非对齐配置不能保证 JDBC 不阻塞。应按实际数据库延迟配置驱动 `socketTimeout`/服务端 `statement_timeout`，并结合重试预算验证，不在缺少运行证据时统一硬编码。

本地恢复测试验证分批后的状态和结果一致性，不能代替生产环境的 checkpoint 耗时验证。
