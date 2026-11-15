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

两个作业均应用 checkpoint flow 配置。默认先尝试对齐，等待超过 10 秒后可转为非对齐；最多同时运行一个 checkpoint。非对齐会将网络中的数据纳入快照，可能增加 checkpoint 大小。需要回退时设置 `checkpoint.unaligned.enabled=false`。

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

## WindowMiningJob 的优化与排查

没有获取到本次 Dinky 超时 checkpoint 的算子分项和失败原因，无法据此认定根因。代码中固定窗口此前对每条事件读取和重写整个窗口 JSON；开启历史模式时还保存日规则使用、固定窗口双通道不读取的完整评分事件、父链和行为聚合。窗口增大时这些工作可能延迟 barrier 或增大快照。JDBC checkpoint flush 也是独立的潜在瓶颈。

本次调整：

- `history.compare=true` 时只保留双通道所需的事件身份、完整命令模板、计数、有限样本及高危累计结果，不保留仅日规则使用的逐条评分事件、父链、主机和 Linux 行为聚合。`history.compare=false` 仍保留完整日规则输入。命中计数、模板和高危原因未采样。
- 可兼容的旧窗口桶在读取时按需压缩，保留原 MapState 名称和序列化类型；未被访问的旧桶不会立即压缩。首次迁移会有额外读写，增量 RocksDB 状态及 checkpoint 不一定立即缩小。
- 压缩状态不能用于 `history.compare=false`；读取到压缩状态时明确失败。模式切换仍应使用隔离状态并从完整基线重放。不要从压缩后的 savepoint 回滚到依赖完整桶的旧版本；回滚应使用升级前 savepoint 或隔离回放，并评估 Kafka/数据库 revision。
- 在 watermark 尚不可能使后续同时段窗口关闭时跳过依赖窗口查询；重复 SEAL 不再次全量扫描。首次有效 SEAL 的全量扫描仍是同步工作。
- JDBC sink 取消与上游算子链绑定；JDBC 仍在自己的 checkpoint 中同步 flush，既不会自动解除下游反压，也不会与 Kafka 原子提交。拓扑调整后须用实际集群的隔离 savepoint 验证恢复，不应依赖跳过未知状态。
- 固定窗口配置 checkpoint 间隔 30 秒、最小间隔 10 秒，并启用 10 秒对齐超时后的非对齐；checkpoint timeout 仍为 600 秒，Kafka transaction timeout 仍为 720 秒。需确认 broker 的 `transaction.max.timeout.ms` 并为连续失败和重启预留余量；非对齐不能中断长时间运行的用户回调。
- 当单次窗口状态读取/写入、定时器回调或 SEAL 重算超过 1 秒时记录 `Slow fixed-window state read`、`Slow fixed-window state write`、`Slow fixed-window timer`、`Slow fixed-window seal recomputation`，不记录事件内容；JDBC 继续记录 `Slow JDBC checkpoint flush`。

部署后按每次 checkpoint 的每个 operator/subtask 记录 start delay、alignment、同步/异步耗时和状态大小，并结合 busy/backpressured、Kafka lag、GC、HDFS、慢状态/慢 JDBC 日志比较同等负载。高 start delay 与反压对应上下游 barrier 等待；同步耗时高时检查用户回调和 sink flush；异步耗时高时检查状态存储与网络。一次 checkpoint 的总耗时和增量大小不能证明是 Flink 状态、HDFS 还是数据库导致。参见 [Flink 1.18 checkpoint 监控](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/monitoring/checkpoint_monitoring/) 和 [反压下 checkpoint](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/checkpointing_under_backpressure/)。

上述历史模式压缩之后，固定窗口状态进一步改为有界分块，见下节。没有线上压测数据，不能保证 checkpoint 超时已经消除。

## 固定窗口有界分块状态

`WindowMiningJob` 的普通输入不再读取和重写整个窗口 JSON。`WindowBucketState` 将每个窗口的新输入按接收顺序分为最多 128 条事件一个聚合块，去重身份用独立的托管 `MapState` 索引，另用小型 header 保存追加数量和身份数。每条接纳事件只更新当前块、一个身份和 header；已完成块不再因后续追加而重写。128 是状态布局常量，现有版本状态不能直接修改该值。

窗口首次关闭、迟到修正、基线重算和 LTS 定时器仍在原来的时点组装完整 `DayBucket`，调用原有规则计算。组装按接收顺序合并全量计数、评分事件、模板、父链和命中原因；展示样本仍按原来的事件时间及 event_id 取相同的有界样本。没有对事件采样，没有合并多条迟到事件的输出，也没有改动 revision、撤回、空快照和 LTS 语义。

保留原 `fixed-window-bucket-v1` 描述符及 operator UID。兼容的旧 checkpoint/savepoint 中，整窗桶作为只读基座保留；旧的历史压缩逻辑仍可一次性改写基座。首次追加时一次性为旧身份建立索引，后续只写新块。未被访问的旧窗口不需要预先迁移。规则版本不兼容或紧凑历史状态用于完整规则时仍明确失败。窗口清理同时删除旧基座、块、header 和身份索引。

新增状态为 `fixed-window-parts-header-v1`、`fixed-window-parts-v1` 和 `fixed-window-identities-v1`。升级后 checkpoint 包含新事件块，不能直接交给只读取整窗桶的旧代码回滚，否则会遗漏新增事件；回滚须使用升级前 savepoint，并处理 Kafka 位点及数据库 revision。升级应在隔离环境验证 savepoint 恢复。

本优化降低持续输入的序列化与重写放大，不保证总状态量下降：身份索引和块内样本有额外存储成本。单事件大小仍无字节上限；首次旧状态索引、完整窗口组装、迟到重算、SEAL 扫描、清理和 JDBC flush 仍同步执行。`keyBy(family)` 的并行边界不变，持续迟到流量仍会逐条触发原有整窗计算。应继续对比同等负载下的 CPU、GC、RocksDB 写入、checkpoint 分项、Kafka lag 和结果延迟。
