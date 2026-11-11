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

`ReplayJob` 是有界本地规则回放入口，不连接 Kafka。测试使用 480 组旧 Python 评分样本中不受新版地址分类影响的用例，并单独测试新的地址规则；还覆盖日桶采样、基线求和、Nginx、Flink checkpoint 恢复、迟到撤回等。`scripts/generate_oracle.py` 可从相邻旧项目重新生成评分金样本，仅调用纯规则函数，不查询外部服务，不写旧目录。

## 日志输出

业务代码使用 SLF4J。日志实现和配置由 Flink 进程提供，胖 JAR 不打包 Log4j 配置。集群启动参数是 `-Dlog4j.configurationFile=file:log4j.properties`，读的是 Flink 配置目录在 YARN 启动时拷进容器的那份（当前为 `/data/flink/flink-1.18.1/ai_conf/log4j.properties`）。Dinky 提交 JAR 不会替换这份配置；改它之后须重启对应 Yarn Session，再提交作业。同一配置目录不要同时放 `logback.xml`。会话集群的日志配置作用于整个进程，包括同进程的其他作业。

文件路径由 JVM 的 `-Dlog.file` 决定。YARN 上 JobManager 是 `jobmanager.log`，TaskManager 是 `taskmanager.log`。若 Web UI 报 `The file LOG does not exist on the TaskExecutor`，表示页面要读取的 `LOG` 不存在，并非这条错误就能证明作业失败。直接看对应 container 目录里的 `taskmanager.log` 或 `jobmanager.log`。

`ReplayJob` 的 JSON 结果仍输出到标准输出，可用 `> /tmp/mining-results.jsonl` 保存；这些业务结果不是运行日志。INFO 保留作业与 Kafka 路由、算子初始化、FLEET 更新和日封账、日窗口基线等待原因变化及就绪、固定窗口首次计算汇总、快照修订和状态清理。固定窗口汇总包含事件数、基线就绪情况与高危/历史信号数；快照日志表示交给下游，Kafka 事务仍待提交。逐条接收、接纳、去重、迟到路由、定时器、刷新细节和未变化快照降为 DEBUG，排查时可按需启用。两个算子的 `lateEvents` 和 `duplicateEvents` Flink 计数器可用于监控，计数随算子重启而重置。

无法解析或越过时钟偏差的输入以 WARN 记录后送入 DLQ；批次迟到汇总和过期封账也以 WARN 记录。身份容量超限会抛错。日志不打印原始输入、信号内容或 Kafka 认证配置；恢复重放时日志可能重复。独立 `java -cp` 回放不自带 Log4j 2 后端。

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

业务日固定 +08:00，基线是前 N 个日历日。为兼容旧查询，`23:59:59.000` 之后的当日亚秒事件被排除。命令结构评分使用三元组的全部事件所需字段，命令模板和父链签名按全量事件累计；采样只用于展示，展示样本不保证包含所有命中证据。同时间戳以 event_id 打破顺序。

日作业和固定窗口的无历史规则模式会在状态中按三元组保存每条事件的评分字段和完整模板、父链计数；固定窗口双通道模式只保存所需的完整命令模板、计数和高危聚合，不保留逐条评分事件。这些状态仍可能显著增加序列化开销、checkpoint 时间和敏感数据保留量。日作业按族和日、固定窗口按族和窗口受 `max.events.per.family.day` 身份数限制，超过时作业失败，不会静默采样；该上限不限制单条事件大小或总字节数，部署前必须按实际日志压测。开启日历史对比时，既有次数门槛、突增筛选、deep 名额和最终输出上限仍然生效，完整特征不保证每个三元组进入评分或最终输出。

状态为 Flink 托管状态（JSON schema v1），包括日桶、去重身份、上次快照、修订号和定时器。固定 operator UID/maxParallelism；兼容的升级从 Savepoint 恢复，不跳过旧状态。持久化 checkpoint 路径和 transaction prefix 必须按环境配置。

行为规则使用规则和主机维度的完整命中计数，历史比较使用同一主机最近三个基线日的最大计数。DNS 高频规则按单主机每天至少 50 次 `nslookup`、`dig` 或 `host` 事件触发，不分析域名特征。横向移动在目标地址过滤后计数；证据样本仍有上限。输出上限仍可能截断信号。

横向移动规则以源 `host_ip`、`username` 和规范化目标 IP 为基线身份，比较最近三个基线日的全部 SSH、SCP、SFTP、rsync 事件，不同主机或用户的历史不会互相豁免；缺少源主机或用户名时不会使用基线豁免。目标必须是有效 IP：IPv4 RFC1918、共享地址 `100.64.0.0/10` 或 IPv6 ULA `fc00::/7`，缺失地址、域名和其他地址不作内网目标判断。实际企业地址范围可能不同，这不是网络拓扑或资产归属判断。

Windows/跳板机的 `novel_external_connect` 原有字段名保留，但只表示存在公网单播候选连接，不表示该地址相对历史是新的。地址判断不执行 DNS 查询，保守地把回环、链路本地、文档网段、组播、无效和其他特殊用途地址视作未知；并不保证公网候选在具体网络中实际可路由。判断范围需随 [IANA IPv4](https://www.iana.org/assignments/iana-ipv4-special-registry) 和 [IPv6](https://www.iana.org/assignments/iana-ipv6-special-registry) 特殊地址登记及本地网络规划定期核验。

Linux 规则只将编辑或安装 crontab、向计划任务目录写入作为持久化线索；读取私钥或 shadow 的规则要求可识别的读取、复制或打包命令。普通 scp、查询计划任务和单独的防火墙端口放行不再视为攻击信号。不再仅凭父链或进程名称自动豁免。命令匹配仍是启发式：可识别的合法操作也可能命中，脚本内部操作、命令变体及其他读取工具可能漏检；应结合资产、权限和进程上下文核验。

**本次规则升级需要从输入重新回放日作业和固定窗口作业。** 旧状态缺少完整命令、模板及父链信息，也无法准确转换早期行为聚合。读取不兼容的非空旧状态时作业会明确失败，不能直接从旧 savepoint 升级。请使用新的消费组、checkpoint 和隔离输出，从所需基线期开始回放并核验后再切换。

两个作业的 checkpoint 超时配置、优化边界与上线验证见 [checkpoint 排查说明](docs/checkpoint-tuning.md)。

## 固定时间窗口

`WindowMiningJob` 与按日作业并行，使用独立消费组。窗口按事件时间对齐到 Unix 纪元，默认 5 分钟；开启历史对比时窗口长度必须整除 24 小时。当前配置 `history.compare=true`，使用两个独立通道：

- `high_risk`：Linux 的 high/critical 行为模式（不包含依赖历史目的地址的规则）以及命中管道执行解释器、编码 PowerShell、可疑 LOLBin 结构的事件，逐条匹配并按窗口计数；不等待历史基线。信号仍受 `window.min.score` 过滤。规则是启发式线索，不等同于确认攻击。
- `history`：按 `baseline.days` 取过去 N 天相同时间段，比较 SAS 三元组次数和全量命令模板，以及 Nginx URI / 新 upstream。三元组和 URI 的突增倍率为目标窗口次数除以 `max(历史窗口平均次数, window.history.baseline.floor)`，阈值为 `window.history.spike.ratio`；目标次数需达到 `min.count`。该通道不计算日作业的 fleet、新主机、父链和历史行为模式，不能视作完整复刻日规则。

历史窗口所属的每天都必须收到同族 `SEAL`，零事件的已封账窗口才视为 0。缺失封账时仅输出高危通道，`baseline_complete=false`；封账到达后重新计算保留的已关闭窗口。上游必须保证同族数据和封账的顺序与完整性，历史导入要覆盖基线期；越过 watermark 允许迟到范围的事件进入 late topic，需离线回放。基线同一时段的聚合保留 N+2 天并使用 Flink 状态和 checkpoint；迟到封账最多影响关闭后保留期内的窗口，超期需要离线重算。全量命中计数与有界样本分开，实际容量、延迟和 checkpoint 需在部署环境压测。

快照包含 `baseline_mode`、`baseline_complete` 和 `baseline_dates`，信号包含 `channel`。窗口第一次输出为 `FINAL`，后续变化为 `CORRECTED`；`FINAL` 是窗口首次关闭语义，不表示历史基线已就绪或迟到期结束。当 watermark 到达窗口结束加允许迟到时间，且历史模式所需的基线均已封账后，作业发送递增 revision 的非空 `LTS` 快照：当前在线计算不再修正该窗口。未开启历史模式时无需等待基线；如果两项条件在首次输出时已满足，则首次快照直接为 `LTS`。基线一直未就绪或 watermark 未推进时不会进入 `LTS`。信号为空的快照不发送 Kafka，也不写 PostgreSQL；如果之前已输出过该窗口，其旧记录和信号不会被撤回，消费者不能将它们视为当前计算结果。Kafka 使用事务 sink；PostgreSQL 按窗口 revision upsert，但与 Kafka 不在同一事务。`LTS` 只代表该次在线计算的终态。JDBC 至少一次写入不与 checkpoint 原子提交，故障回滚后重放可能再次更新数据库；人工 SQL、其他作业或离线重算也不受此标记限制。超期数据仍需按约定离线处理。从旧版本 savepoint 恢复时，升级前已关闭的窗口没有新增的终态定时器，可能不会自动变成 `LTS`；如需覆盖历史行，应使用隔离输出回放并核验，不能仅依据墙钟批量改状态。`history.compare=false` 保持原来的规则模式。切换历史模式应从头回放完整基线，旧模式的 savepoint 不含历史封账和高危计数，不可当作已预热基线。

入口是 [deploy/dinky-submit-window.sql](deploy/dinky-submit-window.sql)，配置是打包进 JAR 的 `window-mining.properties`。账号不能建表时先跑 [deploy/postgres-mining-window-fixed.sql](deploy/postgres-mining-window-fixed.sql)。`kafka.group.id` 和 `kafka.transaction.prefix` 不能与按日作业相同。

## Dinky 部署

1. 复制 `conf/mining.properties.example` 到实际配置位置，设置 Kafka、基线口径及可靠 checkpoint 存储。未配置 `target.start.date` 时，按 +08:00 取作业启动日减 3 天，启动后不再滚动；需要回放更早目标日时再显式填写。checkpoint 使用 `hdfs://nameservice1/user/sls/sls-hunter/flink-mining/checkpoints`，JobManager 和所有 TaskManager 都能访问。
2. `mvn clean verify`，上传带依赖 JAR 到 Dinky 资源中心。
3. 使用 [Dinky 提交脚本](deploy/dinky-submit.sql)，绑定匹配的 Flink 集群。配置路径必须在实际执行节点存在，密钥用受保护配置注入。
4. 以影子输出 topic 回放、核验，再切换消费者。没有在真实 Dinky/Kafka 环境部署或压测。

依赖选择基于 Flink 1.18 / Java 8。实际集群若是其他版本，先匹配 Dinky/Flink/JDK/connector 兼容矩阵，不能只替换一个版本号。Flink runtime/RocksDB 使用 provided；Kafka connector 已打包，不要在集群重复安装另一版 connector。Jackson 已统一版本并隔离包名。 Shade 打包会排除根目录及多版本目录中的 `module-info.class`（作业按 classpath 运行），合并保留 `META-INF/LICENSE`、`NOTICE`、`DEPENDENCIES`，排除输入 JAR 的 `META-INF/MANIFEST.MF`，并由 transformer 合并服务发现条目、生成入口 Manifest。若仍出现其他重复 `.class` 警告，应检查依赖树，不能一律忽略。

## 重要实现边界

首版以 **family 为一致性边界**，在一次定时计算中统一处理 fleet、新主机、deep 预筛、全局排名与撤回，避免遗漏跨键更新。每条事件先写入托管待处理状态，每批按日合并一次并遍历保留的日桶；同族没有跨 TaskManager 分片。这是可运行的正确性基线，**不能据此承诺生产大流量下 1～3 秒延迟**。提高 parallelism 只会并行不同族；高吞吐需将日桶拆为细键增量状态和版本化排名流程，再做容量测试。达到每日身份数上限会明确失败，避免静默丢事件。

当前 Java 日历日主链已实现；旧任意秒级范围中混合查询边界的缺陷没有复制为新的 API。`ReplayJob` 当前同样按日评估。旧 HDFS 五文件和 `agent_bundle` 报告打包层未迁移，输出是新的实时快照协议；它不能直接替代依赖旧报告格式的消费者。ES terms 主机截断、ES wildcard 在不同 mapping 下的索引语义和同分并发排序可能与 Java 全字段匹配存在差异，需用实际冻结输入验收。旧评分样本仅覆盖有限场景，地址分类已有有意差异，不代表全量端到端等价。

[详细目标架构与后续扩展设计](docs/realtime-design.md)描述的是完整演进方案；其中分布式细键状态和任意范围回放不应误认为本版已经实现。
