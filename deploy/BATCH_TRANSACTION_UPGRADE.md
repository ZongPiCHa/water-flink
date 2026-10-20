# MySQL Sink 批次事务升级（2026-10-18）

## 变更

用户、作品 Sink 使用 `BatchTransactionConnectionProvider`，将原先逐条自动提交
改为每次 `executeBatch` 完成后统一提交。沿用 1000 条/1 秒批次、原 SQL 规则保护、
完整快照 UPSERT、算子 UID 和状态结构。失败整批回滚并重试，提交完成后才允许
Checkpoint 确认；仍为 at-least-once。

## 构建与验证

- `mvn -B package -Dprevious.jar=/home/daizixin/flink-apps/water-flink/app/water-flink-before-batch-20261018.jar`
  构建成功；23 项自动测试通过，包括旧部署 JAR 的状态兼容测试。
- 两项需连接 MySQL 的测试在全量构建时跳过，已事先单独执行通过：
  `HeatRuleSinkTest`、`BatchTransactionMysqlTest`。
- 真实 MySQL 验证批次提交可见性、半批失败无部分写入、重连关闭自动提交、
  旧规则快照按当前规则修正。测试数据已回滚或删除隔离表。
- 隔离小表、相同原 SQL、300 行 UPSERT：自动提交 930 ms，批次事务 258 ms，
  本轮约 3.6 倍。不是线上持续负载压测，也未证明生产吞吐可提升相同倍数。
- 部署前 Kafka lag 已为 0、Source 和 Sink 吞吐均为 0，因此不能将积压归零归功于本次修改。

## 部署与恢复

仅更新 application 集群（8082），session 集群（8081）的两个 Hudi 作业继续运行。

| 项目 | 值 |
| --- | --- |
| 原作业 | `dca8e1d97d96395bac8ef68f497b409c` |
| 新作业 | `59ca695127a52788ef4daddff8dbebfe` |
| 停机 Savepoint ID | `2959` |
| 格式 | canonical，`drain=false` |
| Savepoint | `file:///home/daizixin/flink-apps/water-flink/savepoints/savepoint-dca8e1-1500c01ad03f` |
| 新 JAR SHA-256 | `9a6b7254da691c8699e02882c513cc292674c602dd5b22b6bcd1a2896ffd19f9` |
| 旧 JAR SHA-256 | `6f1db9de6370743825e303dee7fd6f9d934daac0d7554be35f2aba898a8c7024` |

部署 JAR：`/home/daizixin/flink-apps/water-flink/app/water-flink.jar`。
备份 JAR：同目录 `water-flink-before-batch-20261018.jar`。
启动配置源文件：`batch-transaction-restore.conf`，
已安装到 `/etc/systemd/system/water-flink-jobmanager.service.d/restore.conf`。

REST 已确认恢复来源为上述 Savepoint，全部作业顶点 ID 和并行度与旧作业一致，
未启用 `allowNonRestoredState`。MySQL `performance_schema.variables_by_thread`
已确认 12 个 `water_flink` 连接的 `autocommit=OFF`。

回退代码时也应先为当前运行作业生成新 Savepoint，再停止服务、更换备份 JAR 并使用
新的状态路径恢复；不能直接重用历史启动配置覆盖之后的消费进度。
本次只改无状态连接包装，状态序列化器保持不变。

## 线上验收（21:35:19，Asia/Shanghai）

- 79/79 子任务 RUNNING，无作业异常记录。canonical Savepoint 约 1.9 GB，
  全部状态加载约 3 分钟；此前增量 Checkpoint 记录的 state size 约 476 MB。
- 新 Checkpoint `2960` 已完成，79/79 子任务确认，耗时 12.254 秒。
  恢复期间曾有一次 Checkpoint 触发失败，日志原因为
  `Not all required tasks are currently running`；不是 JDBC 写入失败。
- 12 个 Sink 连接累计执行 28,172 次 INSERT SELECT、475 次 COMMIT，
  语句错误为 0，均为 `autocommit=OFF`。本轮自然流量下批次常由 1 秒定时器触发，
  不一定积累到 1000 条。以上为采样瞬间的连接级累计计数。
- Source 吞吐约 48 条/秒；用户/作品 Writer 输入约 195/211 条/秒。
  各已分配 Kafka 分区的 Source `pendingRecords=0`，最高反压为 0。
- 已提交位点 lag 为 911：当前 Checkpoint 间隔为 120 秒，包含已读取但尚未随
  下一次 Checkpoint 提交位点的数据；不能把该数直接理解为 Source 尚未消费量。
- 6 个 Prometheus 抓取目标全部正常；8081 两个 Hudi 作业仍为 RUNNING。

## 监控

Grafana：<http://10.37.215.3:3000/d/water-flink-overview>
（服务控制台已有入口）。结合 Kafka 已提交位点积压、Source 吞吐、MySQL Writer
输入吞吐、反压比例和 Checkpoint 耗时观察下一轮自然流量。
Writer 输入速率不等于数据库成功提交速率。
