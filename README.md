# SLS Hunter Flink Mining

独立 Java 8 / Flink 1.18.1 / Kafka DataStream 项目，使用 Dinky 提交 JAR。原 Spark 项目不参与运行。四族规则、进程集合、Windows 资产规则与 Linux 行为查询已移植，纯 Java 运行，不需要 JEP 或 Python。

## 构建与回放

```bash
mvn clean verify
python3 scripts/generate_demo.py > /tmp/mining-demo.jsonl
java -cp target/sls-hunter-flink-mining-1.0.0-SNAPSHOT.jar \
  com.sls.hunter.flink.ReplayJob conf/replay.properties /tmp/mining-demo.jsonl 2026-06-10
```

打包配置启用了 `maven-jar-plugin.forceCreation`：每次先重建仅含项目类和资源的普通 JAR，再由 Shade 生成胖 JAR，避免不执行 `clean` 时把上次胖 JAR 再次作为输入。请使用 `mvn package` 或 `mvn verify`，不要直接反复执行 `mvn shade:shade`。

`ReplayJob` 是有界本地规则回放入口，不连接 Kafka。测试包含 480 组旧 Python 评分对照，以及日桶采样、基线求和、Nginx、Flink checkpoint 恢复、迟到撤回等。`scripts/generate_oracle.py` 可从相邻旧项目重新生成评分金样本，仅调用纯规则函数，不查询外部服务，不写旧目录。

## 日志输出

业务代码使用 SLF4J。日志实现和配置由 Flink 进程提供，胖 JAR 不打包 Log4j 配置。集群启动参数是 `-Dlog4j.configurationFile=file:log4j.properties`，读的是 Flink 配置目录在 YARN 启动时拷进容器的那份（当前为 `/data/flink/flink-1.18.1/ai_conf/log4j.properties`）。Dinky 提交 JAR 不会替换这份配置；改它之后须重启对应 Yarn Session，再提交作业。同一配置目录不要同时放 `logback.xml`。会话集群的日志配置作用于整个进程，包括同进程的其他作业。

文件路径由 JVM 的 `-Dlog.file` 决定。YARN 上 JobManager 是 `jobmanager.log`，TaskManager 是 `taskmanager.log`。若 Web UI 报 `The file LOG does not exist on the TaskExecutor`，表示页面要读取的 `LOG` 不存在，并非这条错误就能证明作业失败。直接看对应 container 目录里的 `taskmanager.log` 或 `jobmanager.log`。

`ReplayJob` 的 JSON 结果仍输出到标准输出，可用 `> /tmp/mining-results.jsonl` 保存；这些业务结果不是运行日志。INFO 记录作业与 Kafka 路由摘要、算子初始化、每条输入的接收、接纳、缓冲、折入日桶、跳过原因、FLEET 更新、日封账、迟到路由、刷新汇总、基线或 fleet 未就绪、目标日早于起始日、窗口评估、未变化快照、过期状态清理、交给 Kafka sink 的记录和输出快照摘要。无法解析或越过时钟偏差的输入以 WARN 记录后送入 DLQ。身份容量超限记录 ERROR 后继续按原逻辑抛错。日志带 topic、partition、offset 和 event id，不打印原始输入、信号内容或 Kafka 认证配置。快照和 sink 日志只表示交给下游，不表示事务已提交；恢复重放可能重复出现同一条输入和同一修订的日志。独立 `java -cp` 回放不自带 Log4j 2 后端。

## Kafka 输入

输入 topic 使用 JSON Lines，每条消息以 `index_family` 作为 Kafka key。**同族 EVENT、FLEET、SEAL 必须由同一有序发布链路写到同一分区**；多个独立生产者发出的 SEAL 无法证明数据完整，不可直接使用。

```json
{"index_family":"tracing_sas_linux","event_id":"source-index:doc-id","payload":{"@timestamp":"2026-06-10T12:00:00+08:00","host_ip":"10.0.0.2","username":"root","proc_name":"curl","cmdline":"curl http://example.org/a","cmd_chain":"[]"}}
```

支持四族：`tracing_sas_linux`、`tracing_sas_win`、`tracing_sas_jumper`、`tracing_nginx_hertzbeat`。Nginx payload 使用 `uri/method/status/upstream_addr`。`event_id` 应稳定表示源 ES 文档身份；也可提供 `source_index/source_id`。无身份、未知族、错误时间等进入 DLQ。只支持 append-only，不支持源文档覆盖更新/删除。

基线历史和目标日志使用同一个作业消费。首次部署从 earliest 回放；从 checkpoint/savepoint 恢复时使用恢复的 offsets。必须保留至少基线天数、目标天和修正期的输入。

完成一天历史导入及各来源核验后，协调发布者在同一族数据后发布：

```json
{"index_family":"tracing_sas_linux","kind":"SEAL","day":"2026-06-09"}
```

即使当天零日志也要 SEAL。目标日前 N 天全部 SEAL 才发布结果，避免冷启动把全部行为当新增。目标日 SEAL 且超过结束时间加 allowed.lateness 才 FINAL；随后到达的保留期内日志修正结果为 CORRECTED。SEAL 是经过上游核验的完整性声明，作业不会凭 watermark 猜测来源完整。超在线保留期日志进入 late topic，需归档回放重算。

兼容 fleet 模式默认 `legacy_snapshot`，保持旧 ES terms 截断与 cardinality 结果。可在配置中加载导出文件，或在同一族流中发布按目标日计算的完整快照：

```json
{"index_family":"tracing_sas_linux","kind":"FLEET","day":"2026-06-10","values":{"curl":12,"bash":80}}
```

没有当日快照则暂停该日输出，不将未知值当稀有。快照字典内缺 proc 按旧逻辑返回 0。快照必须来自目标日前同一基线数据版本，基线修正后同步更新。`scripts/export_fleet_snapshot.py` 是显式调用的旧 ES 查询导出工具；没有自动执行它。可显式选择 `fleet.mode=exact` 去除外部快照依赖，但近似基数/截断阈值附近的结果会改变，需要单独规则版本验收。

## 输出与故障恢复

结果 topic 使用事务投递，下游设置 `isolation.level=read_committed`。每条输出是**原子完整窗口快照**：`operation=REPLACE`、`all_signals`、`shallow_candidates`、`deleted_signal_ids`、`revision`、`status`。按 family+target_day、递增 revision 幂等覆盖；不要把完整快照当成新增告警列表。signal_id 不含分数，可稳定关联同一实体。投递可见延迟包含 checkpoint 提交。

`jdbc.enabled=true` 时，同一条结果快照再写入 PostgreSQL 表 `mining_window`（可用 `jdbc.table` 改名）。Kafka 三个事务 sink 保持不变，DLQ 和 late 不进数据库。表的主键是 `index_family, target_day`，`snapshot` 列保存与 Kafka 相同的 JSON；冲突时只接受更大的 `revision`。JDBC 是至少一次，靠这个条件收成按窗口幂等，和 Kafka 不是同一个事务。作业启动时执行 `CREATE TABLE IF NOT EXISTS`；账号不能建表时先跑 [deploy/postgres-mining-window.sql](deploy/postgres-mining-window.sql)。默认 `jdbc.enabled=false`，避免未填密码就连库。密码放在 `jdbc.password`，不要放进 URL。库不在 `public` 时用 `currentSchema` 查询参数。

业务日固定 +08:00，基线是前 N 个日历日。为兼容旧查询，`23:59:59.000` 之后的当日亚秒事件被排除。采样使用事件时间最早 8/12 条；同时间戳以 event_id 打破不确定顺序。原始日志按天保留身份去重，样本按实体有界保留。

状态为 Flink 托管状态（JSON schema v1），包括日桶、去重身份、上次快照、修订号和定时器。固定 operator UID/maxParallelism；升级从 Savepoint 恢复，不跳过旧状态。持久化 checkpoint 路径和 transaction prefix 必须按环境配置。

`MiningJob` checkpoint 超时的配置、优化边界与上线验证见 [checkpoint 排查说明](docs/checkpoint-tuning.md)。

## 固定时间窗口

`WindowMiningJob` 与按日作业并行，单独的消费组读同一份输入。窗口是按事件时间对齐到 Unix 纪元的滚动窗口，默认 5 分钟（`window.size.ms`）。窗口内命令走 `MiningEngine` 在 `history.compare=false` 时的命令结构规则和 Linux 行为规则，不把空基线当成新增。`history.compare=true` 时作业直接拒绝启动。

窗口关闭后，分数达到 `window.min.score`（含本数，默认 0，即现有规则留下的信号都保留）的结果按分数从高到低打成一条 `REPLACE` 快照。Kafka 写到本作业自己的 `kafka.output.topic`，key 是 `index_family:window_start`。`jdbc.enabled=true` 时再 upsert 到 `mining_window_fixed`（`jdbc.window.table`），主键是 `index_family, window_start`，只接受更大的 `revision`。首次关闭是 `FINAL`，允许迟到内的补录重算为 `CORRECTED`。超过 `allowed.lateness.ms` 的事件进入本作业的 late topic。没有过线信号的窗口不落库。

入口是 [deploy/dinky-submit-window.sql](deploy/dinky-submit-window.sql)，配置是打包进 JAR 的 `window-mining.properties`。账号不能建表时先跑 [deploy/postgres-mining-window-fixed.sql](deploy/postgres-mining-window-fixed.sql)。`kafka.group.id` 和 `kafka.transaction.prefix` 不能与按日作业相同。

## Dinky 部署

1. 复制 `conf/mining.properties.example` 到实际配置位置，设置 Kafka、基线口径及可靠 checkpoint 存储。未配置 `target.start.date` 时，按 +08:00 取作业启动日减 3 天，启动后不再滚动；需要回放更早目标日时再显式填写。checkpoint 使用 `hdfs://nameservice1/user/sls/sls-hunter/flink-mining/checkpoints`，JobManager 和所有 TaskManager 都能访问。
2. `mvn clean verify`，上传带依赖 JAR 到 Dinky 资源中心。
3. 使用 [Dinky 提交脚本](deploy/dinky-submit.sql)，绑定匹配的 Flink 集群。配置路径必须在实际执行节点存在，密钥用受保护配置注入。
4. 以影子输出 topic 回放、核验，再切换消费者。没有在真实 Dinky/Kafka 环境部署或压测。

依赖选择基于 Flink 1.18 / Java 8。实际集群若是其他版本，先匹配 Dinky/Flink/JDK/connector 兼容矩阵，不能只替换一个版本号。Flink runtime/RocksDB 使用 provided；Kafka connector 已打包，不要在集群重复安装另一版 connector。Jackson 已统一版本并隔离包名。 Shade 打包会排除根目录及多版本目录中的 `module-info.class`（作业按 classpath 运行），合并保留 `META-INF/LICENSE`、`NOTICE`、`DEPENDENCIES`，排除输入 JAR 的 `META-INF/MANIFEST.MF`，并由 transformer 合并服务发现条目、生成入口 Manifest。若仍出现其他重复 `.class` 警告，应检查依赖树，不能一律忽略。

## 重要实现边界

首版以 **family 为一致性边界**，在一次定时计算中统一处理 fleet、新主机、deep 预筛、全局排名与撤回，避免遗漏跨键更新。每条事件先写入托管待处理状态，每批按日合并一次并遍历保留的日桶；同族没有跨 TaskManager 分片。这是可运行的正确性基线，**不能据此承诺生产大流量下 1～3 秒延迟**。提高 parallelism 只会并行不同族；高吞吐需将日桶拆为细键增量状态和版本化排名流程，再做容量测试。达到每日身份数上限会明确失败，避免静默丢事件。

当前 Java 日历日主链已实现；旧任意秒级范围中混合查询边界的缺陷没有复制为新的 API。`ReplayJob` 当前同样按日评估。旧 HDFS 五文件和 `agent_bundle` 报告打包层未迁移，输出是新的实时快照协议；它不能直接替代依赖旧报告格式的消费者。ES terms 主机截断、ES wildcard 在不同 mapping 下的索引语义和同分并发排序可能与 Java 全字段匹配存在差异，需用实际冻结输入验收。480 组评分通过仅证明这些用例的规则评分一致，不代表全量端到端已证明等价。

[详细目标架构与后续扩展设计](docs/realtime-design.md)描述的是完整演进方案；其中分布式细键状态和任意范围回放不应误认为本版已经实现。
