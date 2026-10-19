我会结合我们现有的 Doris、Kafka、Flink、Hudi 和监控项目，梳理哪些能力已经有基础，再挑出最值得补做、能形成面试案例的实践。重点放在 Doris 运维和故障恢复，避免只是继续堆组件。

**Considering project requirements**

I need to address a new task that doesn't require executing a proposal. It seems I might need tools for memory and research, but maybe I don't need the web. I wonder if a visual aid is necessary for this educational project? It looks like I should mostly focus on planning and architecture, possibly using dynamic inline tools while avoiding overload. A simple table might be useful, especially for recommendations. I'll read through the tools and assess my research skills for this scenario.

我会用研究分析技能核对项目现状和岗位要求。你前面给出的 Kafka 约 100 QPS，也会作为实践规模的约束：日常链路按这个负载设计，千万级数据和故障场景放在独立实验中验证。

**Clarifying service configurations**

I think I need to skip over creating a visual rational table because it doesn't seem necessary. Instead, I should focus on actually inspecting the Doris services config tables, possibly looking into HTTP and SQL credentials to avoid issues. I should check the system memory, but it feels like I won’t need to take action right now. It’s all about keeping things clear and concise!

**Planning Chinese modules**

I need to create a concise final output of substantial content in Chinese, aiming for about 1,500 words. I’m thinking of organizing it into around 8 modules and prioritizing the most important ones. There’s a timeline of 6 weeks for completion. I’ve got a budget of $9,000 left, which should be sufficient, but I'll need to avoid overhead costs. It would also be helpful to implement actual demo tables and load them for better visualization.

**Budgeting for tasks**

I need to figure out the budget, aiming for something concise. It looks like there are around 9k remaining, but actually 10k tokens are available. So far, we've spent about 7k on tools. I think I can deliver a solid 1500 Chinese words with visuals. I should keep it official and not necessarily web-based. I might need to fetch a few search results for routine backup and workload documents from a single search.

核查发现一个很适合作为学习起点的问题：Doris 目前是单 FE、单 BE；已有的 Kafka 导入任务处于暂停状态，JSON 映射还是旧版事件字段，并有错误行记录。相比继续增加组件，把这条导入链路修好、建模、调优，再完成一次备份恢复演练，会更贴近这份岗位的核心要求。
我会用一张简图把实践方向串起来，再给每个方向列出具体实验和验收标准，方便你把“做过什么”转化为面试时能解释的证据。

**我们现有项目很适合继续做成“Doris 实时数仓运维项目”。最值得补的是：Doris 导入与建模、性能调优、备份恢复、高可用和运维治理。** 这些能直接对应岗位职责，也能形成有数据、有故障过程、有验收结果的面试案例。

目前已有的基础不错：MySQL 业务库、Kafka 埋点、Flink 热度计算、CDC → Hudi、Grafana/Prometheus、服务控制台，以及一次 MySQL Sink 批次事务优化。但 **Doris 这一侧还比较薄**。我刚核查了实际状态：

- Doris 目前是 **1 个 FE、1 个 BE**，可以做功能实验，还不能证明高可用能力。
- `demo.community_events` 已建表，但数据字段还是旧版埋点协议。
- Routine Load `job_events` 当前为 **PAUSED**，有错误行记录；JSON 映射仍使用 `eventId/eventType/designId` 等旧字段。
- 导入期望并发设置为 **256**，Kafka 只有 **3 个分区**，实际并发显示为 3。这是一个很好的“配置与实际负载不匹配”案例。

我建议按下面的顺序扩展。

| 优先级 | 项目实践 | 对应岗位能力 | 做完应留下什么证据 |
|---|---|---|---|
| 1 | 修复 Kafka → Doris 导入，建设社区数仓 | Doris/Kafka 运维、ETL、数仓建模 | 建表设计、导入错误分析、对账结果 |
| 2 | 千万行查询与导入调优实验 | 引擎原理、索引、性能定位 | Profile、执行计划、优化前后结果 |
| 3 | 资源配置与查询隔离 | 内存优化、稳定性、读写隔离 | 资源预算、并发测试、导入延迟 |
| 4 | 备份与故障恢复演练 | 备份、应急处理、灾难恢复 | 恢复记录、RPO/RTO、校验结果 |
| 5 | 多节点 HA 与扩缩容实验 | 高可用、动态扩展 | 故障切换、副本修复、迁移过程 |
| 6 | 控制台增加 Doris 运维功能 | 运维工具、健康检查、治理 | 巡检报告、告警、操作审计 |

**第一项：把 Doris 真正接进我们的业务链路。**

新增独立的数仓库和新版导入任务，按照当前 Kafka 事件结构设计，先在隔离 Topic 验证，再明确生产消费起点。旧任务位点很早，不能简单恢复后就大量回放。

可以做四层数据：

- **ODS 原始事件**：保留事件 ID、事件时间、接收时间、动作、用户和作品 ID、原始属性，支持问题追溯。
- **DWD 明细**：统一字段与时间口径，识别重复、缺失 ID、无效动作和迟到事件。
- **DWS 汇总**：按小时、天、作品、用户统计行为指标。
- **ADS 应用**：社区活跃趋势、作品分析、用户行为漏斗。

这一步要重点学习 Doris 的 **Duplicate Key、Unique Key、Aggregate Key**：分别用在什么场景，为什么这样选，写入、更新、聚合的代价是什么。

验收不能只看“任务 RUNNING”。要检查输入量、有效行、错误行、延迟和重复情况。业务总点赞数继续以 MySQL 事实表为准；事件次数、窗口有效互动和业务当前总数是不同口径，不能直接拿来相减。

**第二项：做一套千万行的可重复调优实验。**

沿用社区事件字段，明确标注哪些是实验数据。日常 Kafka 保持你给出的 **约 100 QPS**；千万行历史数据通过独立批量导入准备，不需要提高线上流量。

挑三类查询：

1. 指定作品、指定时间范围的事件明细。
2. 按天、动作、作品分组的聚合分析。
3. 事件明细关联作品和用户维表。

围绕这些查询对比日期分区、分桶、排序键、倒排索引、物化视图和统计信息。每次只改变一项，记录扫描行数、耗时、峰值内存、执行计划和 Query Profile。区分冷缓存、热缓存，以及单查询和并发查询。

**目标是能解释“瓶颈在哪里、为什么这个改动有效、付出了什么代价”，而不只是说加索引后更快。**

**第三项：把前面提到的内存优化做成资源管理案例。**

100 QPS 是入口负载，不能单独决定内存大小：Flink 状态保留多久、导入批次多大、查询扫描多少数据、多少人并发，都会影响资源。

我们可以先做主机资源预算，分别测量 Doris FE/BE、Flink、Kafka、MySQL 的实际占用，再调整。Doris 重点学习：

- FE 堆使用与 GC，区分配置上限和实际需求。
- BE 查询、导入、Compaction、缓存的内存消耗。
- Workload Group 对交互查询、批量分析、导入任务的资源管理。
- 大查询运行时，实时导入延迟是否仍然可控。

JD 的“读写分离”也值得专门练习：**Doris 不能直接套用 MySQL 主从读写分离的思路。** 先做同集群的查询与导入资源隔离，再研究独立集群或存算分离计算组的适用场景；额外增加 FE 并不意味着 BE 的计算资源就隔离了。

缓存方案也应从明确的查询需求出发，比较 Doris 自身缓存、物化视图和应用结果缓存，考虑失效与数据新鲜度，不必为了岗位里出现“缓存”就先增加 Redis。

**第四项：完成一次真正的备份恢复。**

利用现有 HDFS 或对象存储练习 Doris 备份，在独立库或实验集群恢复，校验表结构、行数及关键聚合结果。官方提供了数据库、表和分区的快照备份恢复能力：[Doris 备份恢复文档](https://doris.apache.org/docs/4.x/admin-manual/data-admin/backup-restore/overview/)。

演练可以从误删实验分区开始，再做损坏实验节点后的恢复。记录：

- 能恢复到哪个时间点，即 RPO。
- 从故障发生到查询恢复需要多久，即 RTO。
- 恢复以后如何补齐 Kafka 后续数据，怎样避免重复。
- 表数据之外，账号、配置、导入任务定义如何恢复。

**同一台主机上的 HDFS 备份可以用于练习恢复，但不能证明整机故障时的容灾能力。**

**第五项：有额外机器后，再做高可用和扩缩容。**

建议实验环境建设 3 个参与选举的 FE、3 个 BE，并验证多副本。练习 FE Leader 故障、BE 故障、副本修复、新增 BE 和退役 BE，观察查询、导入与数据迁移。

同机多进程能帮助学习操作流程；要验证主机级故障，需要不同主机。Kafka 也一样：当前单副本基础无法验证 Broker 故障后的可用性，可以在独立多 Broker 环境学习副本、ISR、`acks`、`min.insync.replicas` 和保留策略。副本解决部分可用性问题，也不能替代备份。

**第六项：把服务控制台扩展成轻量 DBA 工具。**

这很契合你偏好通过日志、指标和状态追踪问题的方式。建议增加：

- Doris FE/BE、Tablet、副本和磁盘健康检查。
- Routine Load 暂停原因、错误行和导入延迟。
- 慢查询及 Profile 入口。
- 备份结果、最近恢复演练记录。
- 只读、导入、管理账号的权限检查与操作审计。

先把巡检和问题解释做好，再增加有审计记录的操作入口。数仓治理方面补充数据字典、指标口径、简单血缘和质量规则即可，暂时不需要部署庞大的治理平台。

**如果只选三件事，我建议先做“导入与建模 → 千万行调优 → 备份恢复”。** 它们最贴合这份岗位，也能利用现有环境完成。HA 和云平台实践随后补充，StarRocks、TiDB、OceanBase 暂时作为原理与适用场景的比较学习。

面试时，每个实验都整理成“问题 → 证据 → 原因 → 改动 → 验证 → 回退”的案例。我们的 Flink Sink 优化已经具备这种结构，接下来要补出 Doris 的案例。实验项目能证明你的动手和分析能力，但应如实区分实验经验与生产经验，尤其是这份岗位要求的五年以上经历及灾难性故障处理经验。