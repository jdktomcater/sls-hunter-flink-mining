# 实时历史差集模型设计

日期：2026-11-04。依据：当前工作区源码，而非仅依据旧设计文档。本文描述目标架构。当前已实现可构建的 Java 作业与测试；实现范围、运行方式及尚未落地的分布式扩展见 [README](../README.md)，不表示已验证生产可用性。

本文保留旧规则审计与早期迁移设计；其中 8/12/30 条样本限制及旧行为规则不代表当前版本。当前检测口径、状态容量和回放要求以 README 为准。

## 1. 结论与边界

采用 Java DataStream 有状态增量计算，Kafka 输入输出，Dinky 管理 JAR 生命周期。按索引族隔离计算，保留 Linux、Windows、Jumper、Nginx 四条规则链。算法不再运行于 Spark Driver/JEP/Python；Python 只用于迁移阶段离线对照。

推荐保留“前 N 个日历日基线 + 当日累计目标”的规则定义，默认基线 5 天、时区 +08:00。实时事件到达后更新当日统计、候选、分数和排名。不能改为 1 分钟窗口对比 5 天总量，也不能把当天新出现的行为立即纳入当天基线，否则分别改变突增和新颖性语义。

必须区分三种输出：

- `PROVISIONAL`：日内当前已知输入下的结果，可更新、撤回；不承诺与全天离线结果相同。
- `FINAL`：目标窗口结束、允许迟到期结束、各输入分区和基线版本完成校验后的结果。
- `CORRECTED`：封账后补到数据触发有界重算的新版本，覆盖原封账结果。

不存在“无限迟到、永不修正、秒级最终输出、全天结果完全一致”同时成立的方案。优化的是首次可用结果的延迟，最终完整性仍受数据到达限制。

## 2. 旧项目实际行为审计

路径均相对 `../sls-hunter-spark-mining/src/main/python/`。

| 主题 | 源码事实 | 迁移要求 |
|---|---|---|
| 编排 | Scala → JEP → registry → family runner → Python ES 查询；SAS 共用 `mining/history_diff_v2_parallel.py` | 这不是一套已分布式执行的 Spark 算法；不能机械翻译 Spark 算子 |
| 主机门槛 | `mine_v2_parallel` 遍历所有目标主机，没有旧文档所述的 30 条/2.5 倍候选门槛 | SAS 不新增这层过滤；包装层传参须逐入口核对 |
| 三元组基线 | 各 SAS `calendar_query.composite_triple_counts_host_multiday` 对多日一次聚合，返回 doc_count 总和 | `baseline_count_max` 虽名为 max，实际为总数；不能改为日峰值或中位数 |
| 突增 | `bc > 0` 时 `tc/(bc+1)`；否则 `tc` | 保留平滑项及两位小数中间舍入 |
| 候选 | 数量达到 min_count，且 novel 或突增 ≥2 或罕见网关进程且 fleet 主机数 ≤25 | 不把候选门槛误用为最终打分门槛 3/10 |
| 全局预筛 | `pre_score_v2` 排序；罕见网关全部进入 deep，其余补足 max_signals | 网关数量可以超过 max_signals；不能硬截断 |
| 样本 | target 最早 8 条；baseline 每日最早 12 条 | 使用事件时间最早 K，不是到达最早 K，也不是随机抽样 |
| 主机信号 | 新主机按目标量取 25，分数 `35+min(events/20,25)` | 要独立于三元组 deep 保留 |
| 最终裁剪 | host + deep + behavior 按分数合并；容量 `max(max_signals+30,200)` | 全量候选不等于最终 all_signals |
| 模板 | `log_template/drain_parser.py` 当前输出实质是预掩码及 token 归一化，parse 返回当前序列形状 | 不替换成其他 Drain 库；即使类名相同也会变规则 |
| 父链 | 解析 list/dict 值；前 4 段逐段归一化、截取 80 字符，使用箭头连接 | 保留顺序、非 JSON 回退和截断规则 |
| Linux 附加规则 | `tracing_sas_linux/behavior_patterns.py`，独立于 triple deep | 不能只迁移历史差集评分 |
| Win/Jumper | 各自 `structural_intrusion_score.py`、`whitelist_rules.py`，Windows 另有资产规则 | 不合并成通用 Linux 规则，不提前删除白名单事件 |
| 稀有网关 | deep 后最终加 85 分，在 family 调整之后 | 加分顺序必须保持，不能在 score cap 前后随意移动 |
| Nginx | `tracing_nginx_hertzbeat/history_diff_mine.py` 独立算法 | 不套 SAS triple 模型 |

### 2.1 必须显式记录的旧行为限制

fleet 稀有度当前来自 ES `terms(size=500)` 加 cardinality：只返回部分 proc 且是近似基数，字典缺失按 0。精确全集去重会修正旧系统但可能改变 1/3/8/25 阈值附近分数。兼容验收时冻结旧 ES fleet 查询结果作为维表；生产切为精确统计须单独升级语义版本并评估差异，不声称逐条等价。主机 terms 查询也有上限，范围路径为 10000，超过上限的数据覆盖差异要独立报告。

时间范围模式存在口径错位：triple 计数使用范围，deep 抽样只传 target_start，host 首日志和 behavior 同样没有完整范围透传；部分基线证据仍按整天。新模型的自动实时主链先限定日历日，任意范围查询作为单独回放任务。范围模式不能一边修正、一边承诺旧输出不变。

旧查询只有 timestamp 排序，同时间戳无稳定次序；Python 并发完成顺序也影响同分 Top 截断。Java 增加 event_id/实体键作为稳定次序，但将这类 tie 差异单列，原系统没有可保证复现的唯一排序。Python round 与 Java 默认舍入及 Unicode/正则行为也须以样例测试固定，不能默认相同。

部分旧函数查询异常返回空集合或吞异常。新模型不得把“基线读取失败”当“基线不存在”，应阻止封账并发出运行告警；这是故障处理修复，不改变正常输入的规则。

### 2.2 SAS 分数契约

令 T 为目标计数、B 为基线多日总计、F 为基线进程主机数、M/C 为样本中新模板/新父链数。

```text
ratioRaw = B > 0 ? T/(B+1.0) : T
候选过滤使用 ratioRaw；候选存储 ratio = round(ratioRaw,2)
base = 25*[novel_host] + 22*[novel_triple]
     + min(3*M,24) + min(4*C,20)
     + (F<=1 ? 18 : F<=3 ? 10 : F<=8 ? 4 : 0)
     + (ratio>=10 ? min(ratio,25) : ratio>=3 ? 2*ratio : 0)
     + min(T/10.0,12) + 8*[B==0 && T>=2]
```

预评分另外保留 novel +30、fleet 分档 25/18/12/6、网关 +40 或 fetch +15、突增及数量项，不能使用 base 代替预评分。之后按原 family 函数顺序进行结构加分、部署调整、白名单调整/封顶，最后罕见网关 +85。所有中间 breakdown 与旧版逐字段对照。

Linux behavior 契约：目标先全量计数判断门槛，再取每 pattern 最早 30 条，再排除进程/父链和执行后过滤；基线只取 `base_dates[:3]` 的每日最大匹配数。突增分子是过滤后样本长度，不是全量计数。随后按 host 分组评分。DNS、横向目的 IP 后过滤的查询上限及取样方式也要迁移；直接全量检测属于新规则版本。

Nginx URI 归一化顺序：去 query → 小写十六进制长段 `/[0-9a-f]{8,}` 替换为 `/<ID>` → 数字替换 `<N>`。不能用通用 `uri_template()` 替代。

- 新 URI：`40+min(2*T,30)+min(template.length/10,10)`。
- 旧 URI：`T/(baseline_total/N)>=host_spike_ratio` 时 `30+min(T/5,25)`。
- 新 upstream：非空、达到 min_count、基线未见时 `35+min(T,20)`。
- URI 保留最早 5 条样本；status/method 统计不是当前输出中的独立告警规则。

## 3. 数据与作业拓扑

```text
采集 → Kafka raw（四族隔离，保留原始字段和源文档身份）
       → Normalize / 校验 / 源身份去重 → normalized Kafka（可回放）
       → keyed 日桶统计与最早样本 ──→ 版本化 baseline 快照
       → 按 family 的候选、deep、behavior / Nginx 评分
       → 全局排名与变更日志 → Kafka signals / snapshots
       → 下游幂等落库、Agent 消费、报告生成
坏数据 → DLQ；超期迟到 → late-events → 同一 Java 引擎有界重算
Dinky → 提交/监控/Savepoint/恢复上述 Java 作业
```

建议先将 normalize 和 mining 分为两个部署单元，mining 每族单独任务，避免一族热点拖慢其他族。历史加载是有界 Bootstrap/Replay 任务，共享 Java 规则内核。日桶/排名可以先放同一个 mining Job 中减少 Kafka 跳转，扩容瓶颈出现后再拆分。Dinky 是管理面，不位于每条日志处理路径。

输入信封：`schema_version,event_id,index_family,event_time,ingest_time,source_index,source_id,payload`。原始 `@timestamp` 是日历归属依据；展示 `timestamp` 不得覆盖它。保留 host_ip、username、proc_name、cmdline、cmd_chain、dst_ip 及 Nginx uri/method/status/upstream_addr。不得无条件 lower-case 所有业务字段，只有旧规则显式转小写的比较才转小写。

去重以旧 ES 文档身份为准：同一身份重投不加计数，不同身份即使内容相同也保留。Kafka offset 只能标识传输记录；内容哈希会误删旧模型本来计入的重复文档。缺身份时必须制定来源策略并标记覆盖差异。源文档若允许 update/delete，输入需提供版本与操作类型，维护减旧加新；否则当前设计只保证 append-only，不能与可变 ES 任意时刻快照完全一致。

## 4. 状态与跨键依赖

| 状态/算子 | Key | 内容与更新 |
|---|---|---|
| HostDay | family,host | 日计数，基线存在性，目标最早首日志 |
| TripleDay | family,host,user,proc | 每日计数、最早 12 条原样本；目标取前 8 条；基线按日合并计数 |
| FleetMembership | family,proc,host | 各日存在标记/引用数，避免跨日相同 host 重复计数 |
| FleetProc | family,proc | 基线主机总数/兼容快照值及版本 |
| FetchParents | family,day | 按旧 family 取样规则生成父路径集，发布完整版本 |
| BehaviorDay | family,pattern | 原始匹配计数、全局最早 30 条、后过滤依赖 |
| NginxDay | family,template 或 upstream | 每日计数、前 5 个样本 |
| CandidateRank | family,target,shard | 候选映射、预评分有序索引、网关集合 |
| FinalRank | family,target | deep/host/behavior 合并有序集合，输出变更与快照 |

一个关键风险：某主机的首次事件会改变该 host 其他 triple 的 novel_host；迟到基线事件会影响相应 host/triple、fleet proc 和父路径集，最终影响许多目标候选。只有 `keyBy(triple)` 不足以保证正确。

采用版本化维度变更流 + 订阅索引（host→活跃 triple，proc→活跃候选，baseline_day→受影响 target）。维度更新发送重算通知；未收到该版本的候选不得参与该版本 FINAL。小型父路径/规则集可 Broadcast；全量主机/三元组基线禁止广播。按 family/day 的完成屏障包含预期分区、offset cut、快照版本、校验统计；空分区也必须确认。

最早 K 用 `(event_time,event_id)` 有序集合，迟到更早样本插入后移除末项并重算小集合特征；不可将模板计数单调累加，因为样本会被替换。保存原日志用于结构/白名单和输出，避免只存模板丢失规则输入。

全局 deep 选择是原规则一部分。保留所有活跃候选可更新索引，不能只保留曾经的 Top200；候选降分时必须补入。分片 TopK 再汇总适用于非网关候选，网关独立全保留；分片/全局都支持删除和排名下降后的补位。选入 deep 的变化产生选入/撤销事件；可以提前计算小样本特征以降低切换耗时，但正式输出成员集仍遵循预筛。最终合并只需保留候选信号而非全量日志，排名算子的容量通过候选数压测。

## 5. 日历、初始化、迟到与回放

1. 以固定 +08:00 划分日桶，D 的基线为 D-1…D-N，不随日内时刻滚动。历史字典缺项与基线未加载必须有不同表示。
2. 初始化从 Kafka 回放 N 天历史及当日到一个已记录的 offset cut，或导入同一 cut 的 baseline 状态；每分区完成后再发布 ready。保留期不足可从 ES/归档导入 Kafka，按源文档身份消除导入和实时接缝重复。
3. 所有分区完成 baseline 后才发布可对外告警的 PROVISIONAL；加载中仅显示 WARMING_UP，不将未知历史当 novel。
4. 日内新事件立即增量评分；候选排名变更可按 200ms～1s 合并。同秒内持续高流量不得反复延期导致永远不输出，使用固定最迟触发时间。
5. watermark 初始乱序预算 5s、空闲检测 30s、日结允许迟到 10min，均为待实测参数。快速通道不必等 watermark；watermark 主要用于事件时间收束。
6. 空闲分区恢复后仍可能发送旧事件；watermark 不是数据完整证明。长时间全空闲也不会自然证明日结完成；结合采集端 offset/时间完成清单与定时检查，清单缺失只能标记待核验，不能强制宣称 FINAL。
7. 允许迟到内更新目标和受影响基线，发送版本化 UPSERT/DELETE。超期事件持久化到 late topic，按 target+baseline 依赖范围执行有界重算并发布 CORRECTED。
8. 日桶事件时间清理与回放保留策略一起设计：至少 N+1 天加允许迟到及已承诺的修正窗口；processing-time TTL 只作保护，不能承担业务日历到期逻辑。超出在线状态保留期从归档恢复，不能静默丢弃。

旧 SAS 单日边界与 Nginx 使用 `23:59:59` 上界的行为需边界样本对照。推荐新规则统一半开区间 `[00:00:00, next 00:00:00)`；若补入旧查询漏掉的亚秒日志，必须作为版本化覆盖修复记录，不能混入严格兼容验收。

任意历史范围需要保留原始事件或足够细粒度可重建数据；仅日桶无法回答任意秒级截断。范围模式用独立有界 Job 读取归档/指定 Kafka offsets，避免污染实时日桶与当前基线，计算前固定 target/baseline 边界和版本。

## 6. 输出和投递一致性

统一结果头：`signal_id,family,target_start,target_end,baseline_version,rule_version,revision,status,operation,event_time,computed_at`，正文保留旧 signal_type、score、score_breakdown、compare、counts、templates、chains、samples。snapshot 保留旧 stats、all_signals、shallow_candidates 和 evidence/agent_bundle 适配所需内容。

signal_id 是 family + target + signal_type + 业务实体（behavior 加 label，URI 使用完整模板）的稳定编码/哈希，不能包含 score。revision 使用可恢复状态中单调递增版本，同一信号支持 UPSERT 与 DELETE；消费者按版本幂等应用。退出 Top/deep 必须发撤回。每日快照另有窗口版本和完成标志，避免把部分更新当完整集合。

默认可靠通道：checkpoint + Kafka transactional sink，消费者 `read_committed`；transactional.id 前缀按环境、Job 唯一，事务超时覆盖检查点与最长计划恢复并受 broker 上限约束。Exactly-once 保护的是传输与状态恢复，不自动解决源重复、多个 Job 快照原子性或外部数据库一致性。

若事务可见性达不到时效目标，增加明确标识的低延迟暂定通道：至少一次投递 + revision 幂等，不能用于不可撤销动作；可靠通道负责对账收敛。两通道使用相同 signal_id，恢复重发不可导致重复业务动作。

## 7. 性能目标与调优

缺少实际 EPS、活跃主机/三元组数、日志大小、迟到分布和硬件，因此以下是压测目标，不是已经测得的承诺。

| 指标 | 初始目标/建议 |
|---|---|
| 正常负载，Kafka 已写入到暂定输出 P95 | 1～3s（可含短周期排名合并） |
| 可靠结果可见 P95 | 5～15s 起测，包含 checkpoint 提交，不是只有算子耗时 |
| checkpoint 周期 | 先以 5s 压测；依据持久化耗时与反压调整 |
| 日结时间 | 至少窗口结束 + 允许迟到 + 完成清单校验 |
| 日常冷启动 | 不接受直接忽略历史上线；回放速度须高于持续输入速度 |

主要收益来自消除逐 host ES 查询、线程等待与重复抽样。热路径仅更新 keyed state 和少量样本特征，不同步查 ES/MySQL；规则/资产用带版本维度流。首次过门槛、样本变化、fleet/基线版本变化立即重算；仅计数变化可短周期合并。不要假设分数达到上限就永久停止更新，排名和证据仍可能变化。

高基数状态使用适合实际 Flink 版本的磁盘状态后端和增量 checkpoint，持久化存储选共享可靠存储；不是 TaskManager 临时盘。固定 operator UID/maxParallelism 并以 Savepoint 验证 rescale。热点计数可分片求和，但最早 K 与主机去重需可合并且避免重复；不对所有指标盲目加盐。

容量估算：`daily_triples*(N+1)*每桶字节 + samples + host/proc成员关系 + 排名候选 + 去重身份状态`；去重状态可能接近保留期原始事件数，需优先由上游提供唯一一次规范化投递或外部紧凑身份索引。上线前测实际序列化后大小、checkpoint 时长和恢复峰值，不只估 JVM 堆。

监控至少包含 Kafka lag、event_time→ingest_time、ingest→compute、compute→committed、watermark lag、基线完整性、迟到率、重算积压、活跃候选、状态大小、checkpoint 耗时/失败、DLQ、排名撤回次数。

## 8. Java 项目模块和 Dinky 部署

建议单 Maven 项目按包分层，后续有独立发布需求再拆模块：

```text
com.sls.hunter.flink
  MiningJob / BootstrapJob / ReplayJob
  model         输入、信号、维度版本、窗口完成清单
  normalize     字段适配、身份、时间、坏数据侧输出
  state         日桶、最早K、基线快照、依赖通知
  rules.common  tokenizer、chain、preScore/baseScore
  rules.linux   structural、behavior
  rules.win     structural、whitelist、assets
  rules.jumper  structural、whitelist
  rules.nginx   uri、upstream
  ranking       deep选取、全局最终排名、撤回
  output        Kafka变更、快照、旧Agent证据适配
```

Flink/Java/Kafka connector 版本必须先读取实际 Dinky 和 Flink 集群版本及支持矩阵再锁定；不能据旧 Spark Java8 配置推断 Flink 运行版本。Flink runtime 依赖 provided，连接器按集群类路径策略仅装载一次；自有 JSON 依赖按需 shade，防止类冲突。不复制旧项目的连接密码或 decrypted profiles。

通过 Dinky 资源中心上传版本化 JAR，设置 main-class、非敏感配置路径/环境，绑定现有集群，部署四族任务及 normalize/bootstrap。会话或应用部署模式按现有基础设施选择，不擅自引入 Kubernetes。提交脚本见 `../deploy/dinky-submit.sql`；当前实现的实际部署条件见 README。

发布流程：CI 构建与规则对照 → 测试集群回放 → Savepoint → Dinky 切换 JAR 并从兼容状态恢复 → 校验 offset、基线和版本 → 扩流。状态 schema 不兼容时用迁移任务，不能通过忽略未映射状态假装恢复成功。回滚同时回滚代码与兼容 Savepoint，处理输出版本，避免旧数据覆盖新结果。

官方参考：[Dinky 1.1 EXECUTE JAR](https://dinky.org.cn/docs/1.1/extend/expand_statements/execute_jar/)、[Flink Kafka connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)。示例和参数须以实际部署版本复核。

## 9. 迁移验收与实施顺序

阶段一：固化规则清单和离线 oracle。用相同不可变输入导出真实 ES 查询结果，包括 fleet、样本和计数，不把既有周报当唯一真值。枚举所有 family 白名单分支、结构分支、behavior 查询与后过滤、证据打包 schema，建立逐规则黄金用例。迁移版使用 Java 纯函数，测试中调用旧 Python 对照，不在生产引入 Python。

阶段二：完成 normalize、Bootstrap、日桶与纯函数移植。按四族分别验收，未实现的族不得打通用占位评分上线。Java/Python 对照涵盖正则、字典顺序、布尔查询/缺失字段、非 ASCII 截断、舍入、空 cmdline、非法 chain、URI 特殊值。

阶段三：接入跨键通知、全局 deep/最终排名、撤回、迟到重算及可靠输出。Dinky 测试集群执行完整重放，模拟 failover/checkpoint/savepoint/rescale。

阶段四：影子运行覆盖至少完整基线周期及目标日，比较逐候选、分数、breakdown、deep 成员、host/behavior、TopK、Agent evidence。相同输入且冻结 ES 近似维表的正常确定性样本必须一致；同分、近似基数、查询上限、时间缺陷修复的差异分类记录，禁止用总告警数接近冒充等价。

必测反例：

- B=10、T=22 应 ratio=2；B 不能错误变为某日 max。ratio 舍入跨越打分边界单独验证。
- 当日第 9 条记录是新命令：最早 8 条兼容规则不能因此产生新模板。
- 同主机跨 5 天重复运行进程：fleet 算一个主机；基线日到期后仍有其他日记录不能减为零。
- 新主机后来补入昨日事件：主机和相关 triple 新颖性撤回，排名补位。
- 网关候选超过 max_signals：全部进入 deep；最终输出仍遵守原 out_cap。
- 第 31 个 behavior 匹配不是原始最早 30 条：不能擅自增加命中输出；排除和采样顺序保持。
- Nginx 数量达到 2、URI 查询串变化、数字/十六进制段、upstream 为空，分支分别验证。
- 日界线亚秒、相同 timestamp、空闲分区恢复、Kafka 重投、历史导入接缝、规则版本切换、故障恢复后无重复业务动作。

生产参数落地前还需采集：现有 Dinky/Flink/JDK/Kafka 版本、源 topic schema/主键与更新语义、日量及峰值 EPS、活跃实体数、源保留期、迟到分布、目标消费者是否支持撤回、可靠存储路径。上述缺失不阻止本设计成立，但阻止承诺延迟数值和选择最终依赖版本。
