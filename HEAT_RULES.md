# 热度指标与状态升级

## 数据口径

`view_page` 统计作品访问次数（PV），归属 `properties.project_id` 和
`properties.author_user_id`。一次有效事件增加 1，不做用户、session 或 log_id 去重；
重复上报会重复计数。它不经过点赞/撤销状态机，也不会因为事件乱序被当作旧关系丢弃。
依然受事件时间窗口和 5 分钟 allowed lateness 的约束。

新默认显示权重：like=1、comment=3、share=5、follow=4、view_page=0.1。
MySQL/Kafka 的权重和热度使用千分位整数，view_page=100。

新版本部署前已被旧代码忽略的浏览事件不会因恢复状态而重新计算。
历史结果的 `view_page_count=0` 表示此前未采集，并不证明此前无人访问。

## 可扩展范围

链路仍为 MySQL `heat_score_rule` → 发布器 → Kafka
`water.heat-score-config.v1` → Flink 广播状态。

权威配置为 `heat_score_rule.weights` JSON，不再为每个指标增加物理权重列。
`view_page_weight` 是从 JSON 读取的虚拟生成列；旧四列为兼容旧记录/工具保留，
新发布器同时填充它们，但新版计算只读 JSON。修改旧列本身不会修改新版公式。

Kafka 记录示例（完整快照，不是局部 patch）：

```json
{
  "schema_version": 2,
  "version": 4,
  "weights": {
    "like": 1000,
    "comment": 3000,
    "share": 5000,
    "follow": 4000,
    "view_page": 100
  }
}
```

公式为 `Σ(指标累计值 × 权重)`。缺少的计数按 0，未配置的指标不贡献热度。
这是可扩展的加权求和，不执行任意脚本或 SQL 表达式。
版本必须递增；同版本和更旧的消息不会覆盖已启用规则。
新版仍能读取旧格式消息及旧 savepoint 中的规则。

控制台 `/api/heat-score` 接收显示单位：

```json
{"weights":{"like":1,"comment":3,"share":5,"follow":4,"view_page":0.1},"note":"说明"}
```

控制台按配置动态生成字段。发布时验证 1–64 个合法指标、有限非负权重、最多三位
小数，以及两个结果表是否都有对应的 BIGINT `<metric>_count` 列。
没有 Kafka 发布，直接改 MySQL 不会让运行中的 Flink 自动刷新。

发布沿用原来的策略：事务内启用新规则并重算 `window_end >= NOW()-5min` 的结果，
随后发 Kafka 并填写 `published_at`。更早的已关闭窗口不重算；广播消息本身不会触发
无新事实的窗口重发。发布跨 MySQL/Kafka 并非原子事务，若 Kafka 失败须重发同一版本
的完整消息并确认 `published_at`，不要只改数据库认为已经生效。两百万级活跃结果
的重算可能耗时较长，因此该操作单独使用 600 秒超时，普通读取仍为 30 秒。

JDBC INSERT/UPSERT 读取当前启用规则版本：若输入快照版本落后，就按该快照的完整
计数和数据库当前 weights 重算，再写入当前版本。新增行与已存在行都适用，防止
“MySQL 已重算，但旧广播版本的结果晚到”造成计数已更新、热度未更新。正常版本仍用
Flink 算出的分数。这个保护增加了每次写入对单行配置的读取；INSERT SELECT 也可能
减少 JDBC 驱动原有的批次合并机会，大吞吐场景需单独压测。它不解决任意旧事实快照
覆盖新事实快照的问题；当前依赖既有的分区/写入顺序。

## 下一次增加指标

1. 对携带 project_id、author_user_id 的普通项目事件，解析器支持合法 action 名
   `[a-z][a-z0-9_]{0,47}`。除已有点赞/撤销、评论/删除、关注、分享外，默认按次数累加，
   存在窗口状态的 `Map<String,Long> extraMetrics`。没有配置权重时也保留计数。
   业务应确保 action 是有限且固定的名称，不要把随机 ID 拼在 action 中。
2. 如 `download` 使用上述加一语义，通用解析、聚合、计算不需要再写分支。
3. 显式执行 DDL 给两个结果表加 `download_count`，按查询需求决定索引；在维表关联
   的 Row 投影及 JDBC sink 中增加该列映射。这个持久化变更仍需构建和状态恢复升级。
4. 在 weights 中加 `download` 即可启用系数；后续调整/移除公式项不需要重启任务。
5. 若指标需要 UV、时长求和、撤销、去重或新的归属关系，必须先实现它的统计语义。
   不应把这些复杂动作当普通 PV 计数。配置公式不能代替事实采集逻辑。

字段扩展是 DDL（ALTER TABLE），修改记录才是 DML。

## Checkpoint / savepoint 兼容边界

两者都包含 source offset、窗口累计值、关系状态、定时器和广播状态。它们保存运行
进度，不保存“重新执行新业务逻辑所需的全部历史原始事件”。

| 变更 | 恢复建议 |
| --- | --- |
| 仅换权重、增加已有指标的公式项 | 广播热更新，不用恢复或重启 |
| 增加结果表列、修改无状态 sink 映射 | 通常可恢复；数据库 DDL 必须先完成 |
| Flink POJO 增加字段，类名及原字段类型不变 | 支持的序列化器可进行 schema evolution；旧值没有新字段，要处理 null/0 |
| 增加新状态描述符 | 一般从空状态开始，原状态可保留 |
| 改并行度 | 在支持重分布、maxParallelism 兼容的条件下通常可以 |
| 改 UID、状态名、包名/类名，或不兼容的字段类型/序列化器 | 不能假定能直接恢复；需迁移和实际恢复测试 |
| Kryo 泛型类随意改结构、切换不兼容 Flink/connector/backend 版本 | 不能仅凭能编译判断；检查序列化和版本升级支持 |
| 改 keyBy、key 类型、hash、maxParallelism | 涉及状态分区，需专门迁移；即使启动成功也可能读错关系 |
| 改窗口长度、时区、统计定义、事件过滤/去重语义 | 可能能反序列化，但旧累计值口径不再正确；应迁移或重放重算 |
| 删掉有状态算子 | 默认可能无法映射旧状态；allowNonRestoredState 是丢弃状态，不是修复兼容 |
| 状态文件被清理、Kafka 对应 offset 已超保留期 | 不能保证完整恢复和续算 |

计划升级优先 **stop-with-savepoint（canonical，drain=false）→ 新 JAR 恢复 → 验证新
checkpoint**。checkpoint 常用于同一作业故障恢复，也可以在兼容条件下升级；原生格式、
非对齐 checkpoint 中在途数据、连接器事务状态会带来额外约束。savepoint 本身不会
使不兼容的业务逻辑自动兼容。

此次保留算子 UID、状态名、类名及原四个计数，不将旧 POJO 替换成另一种结构，
只增加 extraMetrics/weights。`StateEvolutionTest` 从实际旧部署 JAR 生成序列化
快照，并在新类下读取，验证旧计数保留、新字段为空：

```bash
mvn test -Dprevious.jar=/path/to/previous-water-flink.jar
```

部署的具体 savepoint 见 `deploy/view-page-restore.conf`。该文件仅供本次升级恢复，
下次升级必须重新生成，不能反复从同一个历史 savepoint 覆盖新运行进度。

本次已先以 canonical savepoint 验证新增状态字段的兼容恢复；后续仅修复无状态
JDBC 写入逻辑时使用同一 Flink/backend 下的 native savepoint 缩短恢复时间。
`HeatRuleSinkTest` 通过实际 MySQL 验证旧规则快照的插入/更新保护，并回滚测试数据：

```bash
VERIFY_MYSQL_SINK=1 mvn test -Dtest=HeatRuleSinkTest
```

该测试从环境读取 MYSQL_JDBC_URL、MYSQL_USER、MYSQL_PASSWORD。

## JDBC 批次事务

用户和作品两个 Sink 保留每批最多 1000 条、每 1 秒刷新以及 Checkpoint 前刷新的
行为。连接建立及重连后设置 `autoCommit=false`，每次 `executeBatch` 成功后统一
`commit`，提交成功才允许 Flink 清空批次缓存。SQL 仍保留当前规则版本保护；
此修改减少事务提交次数，不保证将 INSERT SELECT 合并为一条多行 INSERT。

部分语句失败或提交报错时，先尝试整批 rollback，关闭连接，再由原 JDBC Writer
重建连接并重试完整批次，最多重试 3 次。重试仍失败会阻止 Checkpoint 成功，
不能确认一个尚未完成数据库提交的批次。

该模式仍是 **at-least-once**，不是 XA/exactly-once。提交成功但响应丢失时可能重放
整批；计数字段继续使用完整快照覆盖，不改成增量加法。`updated_at` 会随重放更新，
且任意旧事实快照覆盖新事实的既有顺序约束仍然存在。两个 Sink 各自提交事务，
不保证用户表与作品表在同一事务中原子更新。不修改 MySQL 的刷盘持久化参数。

本次没有修改算子 UID 或状态序列化器，通过 canonical stop-with-savepoint
（`drain=false`）恢复；部署记录见 `deploy/BATCH_TRANSACTION_UPGRADE.md`，
恢复参数见 `deploy/batch-transaction-restore.conf`。后续升级仍须生成新的 Savepoint。

自动测试覆盖批次大小、定时刷新、Checkpoint 刷新、半批失败回滚并整批重放、
提交响应丢失以及永久失败。真实 MySQL 的 `BatchTransactionMysqlTest` 使用临时隔离表
（结构与作品结果表相同），验证原 SQL、规则修正、事务可见性和重连事务模式：

```bash
VERIFY_MYSQL_SINK=1 MYSQL_SINK_TEST_TABLE=flink_sink_batch_verify_20261018 \
  mvn test -Dtest=BatchTransactionMysqlTest
```

运行前须创建合法数字后缀的隔离表并保证为空，运行后删除；测试还需要上述三个
MYSQL 环境变量。不要将该变量指向业务结果表。
