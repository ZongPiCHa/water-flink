# 评论刷屏/骚扰风控：双流 Join + CEP（2026-10-20）

## 业务口径

“发评论 → 很快自己删掉”，同一用户短时间内反复出现，典型于刷屏引流、
骚扰作者后删证据、试探敏感词。单次删评正常，重复才告警。

1. **双流 Interval Join**：`comment` 流与 `delete_comment` 流按 `comment_id`
   做事件时间区间关联，删除时间 ∈ [评论时间, 评论时间 + 5 min]。
   删除人必须是评论人本人；作者/管理员删他人评论计入
   `comment_foreign_deletes` 指标，不算撤回。
2. **CEP**：撤回流按 `user_id` 分区，模式 `retract.times(3).within(10 min)`，
   `skipPastLastEvent`，一次连续 6 次撤回产生 2 条告警而不是全部组合。
3. 告警写 MySQL `water_analytics.comment_risk_alert`，主键
   `(user_id, first_comment_id)`，at-least-once 重放幂等覆盖。

参数（环境变量，均有默认值）：`COMMENT_RETRACT_WITHIN=5 min`、
`COMMENT_RISK_COUNT=3`、`COMMENT_RISK_WINDOW=10 min`、
`FLINK_COMMENT_RISK_PARALLELISM=2`。

## 资源

不新增作业、不新增 slot、不改 TaskManager 内存：作为新分支加入原作业，共用原
Kafka Source 和水位线，复用已在 `lib/` 中的 `flink-cep-2.0.2.jar`（pom 中为
provided）。Join 状态最多保留 5 分钟，CEP 部分匹配 10 分钟后清除，自然流量下
约 1.3 条评论/秒，状态为 MB 级。原有算子 UID、并行度、状态结构不变。

## 流量画像（上线前 10 分钟采样）

28,867 条事件，787 次删评全部能匹配到 10 分钟内同一用户的评论，
存活时长 p50 204 ms / p90 310 ms / max 615 ms；10 分钟内撤回 ≥3 次的用户 7 个。
因此 5 分钟 Join 上界有足够余量，3 次/10 分钟阈值预期每 10 分钟几条告警。
模拟流量的删评随机分布，并非真实刷屏行为，告警只证明链路生效。

## 部署

| 项目 | 值 |
| --- | --- |
| 原作业 | `59ca695127a52788ef4daddff8dbebfe` |
| 新作业 | `62995bec76a9e9718cc2cf338cdd0da4` |
| Savepoint（native，drain=false） | `file:///home/daizixin/flink-apps/water-flink/savepoints/savepoint-59ca69-73c82a6dad30` |
| 新 JAR SHA-256 | `03c7e18881933fccaebd2308ee30b472ff0a18c9c5c42d3ee80ecbb0d62ef620` |
| 备份 JAR | `app/water-flink-before-comment-risk-20261020.jar` |
| 启动配置 | `comment-risk-restore.conf` → `/etc/systemd/system/water-flink-jobmanager.service.d/restore.conf` |

DDL `sql/004_comment_risk_alert.sql` 由 root 执行（`water_flink` 账号无 CREATE 权限，
已有 `water_analytics.*` 的 SELECT/INSERT/UPDATE 足够写入，未改授权）。

## 验证

- `mvn -B package -Dprevious.jar=<部署前 JAR>`：新增 `CommentRiskDetectionTest` 3 项
  （命中告警；超时删除/他人删除/稀疏撤回不告警；突发不重叠告警）通过，
  原有测试含状态兼容测试通过，两项 MySQL 测试按惯例跳过。
- 恢复后 18/18 顶点 RUNNING，无异常；恢复来源为上述 Savepoint，新 Checkpoint
  正常完成（约 2 秒，state ≈ 464 MB，与升级前同量级）；TaskManager 仍 6 slot。
- 约 10 分钟自然流量：Join 输出 322 条撤回，CEP 输出并写入 5 条告警，
  均为 3 次撤回、跨 3 个作品，平均存活 137–240 ms。

## 查询

```sql
SELECT user_id, retract_count, project_count, avg_lifetime_ms,
       first_comment_at, last_deleted_at, comment_ids
FROM water_analytics.comment_risk_alert
WHERE last_deleted_at >= NOW() - INTERVAL 1 HOUR
ORDER BY last_deleted_at DESC;
```

回退：先对当前作业做新 Savepoint，换回备份 JAR，并以
`--allowNonRestoredState` 从该 Savepoint 恢复（新算子状态需要丢弃）。
