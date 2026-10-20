# Session Guardian：智能客服全生命周期实时体验风控与归因（双流 Join + CEP）

> 状态：设计稿 v1（2026-10-20）
> 适用：抖音生活服务智能客服，业务线 = CRM / 达人 / 商家自助
> 依赖：Flink 2.0.2（DataStream + flink-cep，两个集群的 `lib/` 均已自带
> `flink-cep-2.0.2.jar`）、Kafka（10.37.215.3:9092）、MySQL（规则配置）、Doris（明细与数仓）

---

## 0. 背景与目标

智能客服已经完成：RAG 优化、精排模型优化、多路召回优化、链路耗时埋点、
multi-agent 架构升级。但这些优化目前缺少一条**实时、以用户一次服务为主线、
能把"用户体验"和"机器内部证据"对齐**的数据链路。

Session Guardian 回答一个问题：

> 用户这一通服务，在哪一环开始变差？是首响慢、机器人答非所问、
> agent 之间兜圈子，还是结案后重复进线？变差能否实时干预？
> 能否归因到具体的召回路 / 模型版本 / agent 路由策略，并回流 badcase？

设计目标：

1. **贯穿生命周期**：进线 → 首响 → 多轮解决 → agent 路由 → 结案评价 → 复联。
2. **双流 Join**：把"用户看到的会话"与"机器内部的召回/排序/RAG 埋点"
   在轮次（turn）粒度对齐。
3. **CEP**：用事件序列模式识别"答非所问循环""agent 转圈""灰度版本连续劣化"
   等聚合告警难以表达的行为，用超时模式识别 SLA 违约。
4. **闭环**：实时转人工干预 + 证据明细入 Doris + badcase 自动回流标注/训练。
5. **资源克制**：状态全部有 TTL 上界；不新增大数据量常驻状态；
   跨天口径放 Doris，不强压给 CEP。

非目标：不替代离线满意度模型；不做坐席侧 UI；不直接修改线上模型。

---

## 1. 总体架构

```
                          Kafka
 ┌─────────────────┐   cs.session.events.v1     ┌──────────────────────────────┐
 │ 客服网关/会话服务 │ ───────────────────────►  │ S1 会话事件源（event time）   │
 └─────────────────┘                            └──────────────┬───────────────┘
                                                               │ 过滤/分流
 ┌─────────────────┐   cs.trace.spans.v1        ┌──────────────▼───────────────┐
 │ RAG/召回/精排/    │ ───────────────────────►  │ S2 链路 span 源（event time） │
 │ agent 各模块埋点  │                            └──────────────┬───────────────┘
 └─────────────────┘                                           │
                 MySQL cs_quality_rule                         │
                        │ Kafka cs.quality-rule-config.v1       │
                        ▼                                       │
                 广播规则状态（版本化）◄────────────────────────┤
                                                               │
        ┌──────────────────────────────────────────────────────┘
        ▼
 ① 双流 Interval Join（key=turn_id，span ∈ [-30s, +2min]）
        │   迟到/缺失 → 侧输出（埋点覆盖率指标）
        ▼
 ② turn 证据聚合：一条 turn 的多条 span 折叠为 TurnEvidenceSnapshot
        │
        ├──► ③ CEP 规则引擎（R2 答非所问循环 / R3 agent 转圈 / R4 灰度退化）
        │            │
        │            ▼
        │   RiskAlert（带证据链 + 规则版本 + 模型版本）
        │
 ┌──────┴───────────────────────────────────────────────┐
 ▼                                                      ▼
 Kafka cs.intervention.command.v1              Kafka cs.quality.alerts.v1
 → 坐席调度/工单（实时转人工，幂等）            → Doris（明细/数仓）+ badcase 回流队列

 S1 独立分支：R1 首响超时（processing-time 定时器，不依赖慢埋点水位线）
 Doris 准实时：R5 复联/一次解决率（7 天口径，不用 CEP 扛长窗口）
```

关键分层决策：

- **Join 产出"证据"，CEP 判断"序列"**。CEP 规则里"答非所问"的判断不只看用户
  点踩，还必须能引用机器侧证据（rerank top score、各路召回命中数、兜底标记）。
- **事件时间与处理时间分工**：依赖埋点证据的规则走 event-time CEP；
  纯 SLA 超时（首响）走 processing-time 定时器，避免被晚到埋点的水位线压住。
- **实时归实时，长周期归数仓**：CEP 状态窗口上限为小时级；7 天一次解决率
  在 Doris 做。

---

## 2. 输入流契约

> 下表是建议契约，**字段名需与现有链路埋点 SDK 对齐**（开放问题 Q1）。
> 埋点已有"链路耗时埋点"基础，预期只需补齐版本/召回路命名字段。

### 2.1 会话事件流 `cs.session.events.v1`

粒度：一个用户动作或一个服务状态迁移一条。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| event_id | string | 去重键（SDK 生成，重试不变） |
| event_time | long | 事件发生毫秒时间戳 |
| session_id | string | 一通服务 |
| turn_id | string | 一轮问答（用户一次提问 + 机器人一次回复） |
| biz_line | string | `crm` / `talent` / `merchant` |
| user_id | string | 进线用户 |
| event_type | enum | `session_start` `user_message` `bot_reply` `agent_handoff` `agent_reply` `session_end` `rating` `intervention_result` |
| intent | string | 意图识别结果（规范后的意图 ID） |
| from_agent / to_agent | string | agent 转接事件的起止 agent |
| reply_mode | string | `normal` / `fallback`（兜底话术） |
| rating | int | 结案评分 1–5（rating 事件） |
| trace_id | string | 与 span 流的关联冗余字段（排障用） |

示例：

```json
{"event_id":"e1","event_time":1792470000100,"session_id":"s1","turn_id":"t1",
 "biz_line":"talent","user_id":"u9","event_type":"bot_reply","intent":"refund_rule",
 "reply_mode":"normal"}
```

### 2.2 链路 span 流 `cs.trace.spans.v1`

粒度：一次 turn 内部每一个被埋点的模块调用一条（一对多）。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| span_id | string | 去重键 |
| event_time | long | span 结束时间 |
| session_id / turn_id | string | 关联键，**turn_id 必填**，是 Join 前提 |
| span_type | enum | `recall` / `rerank` / `rag_generate` / `agent_dispatch` |
| recall_channel | string | 召回时：`faq`/`es`/`vector`/`kb_graph`/... |
| hit_count | int | 该召回路命中条数 |
| latency_ms | long | 本段耗时 |
| model_version | string | 精排/生成模型版本（如 `rerank-20261015-gray2`） |
| top_score | double | rerank 首条分数 |
| fallback | boolean | 是否走了兜底/拒答 |
| tokens_in/out | int | 生成段 token |

### 2.3 时间特性约定（决定 Join 与水位线）

- span **允许晚到**：`bot_reply` 先发出、span 后上报，预期 P99 ≤ 30s，
  Join 上界给 2 min 余量；span 也可能先于 reply 事件到达，下界给 -30s。
- 上线第一阶段先实测分布：span 到达延迟直方图、turn_id 覆盖率、
  span 重复率。这些数字是调 Join 区间和 TTL 的依据，不靠拍脑袋。
- 两个源都设置 20s 乱序容忍 + `withIdleness(1 min)`，
  防止某业务线无流量时分区卡死整体水位线。
- 事件按 `event_time` 解析；无法解析的进脏数据侧输出并计数，进死信 topic 而非静默丢弃。

---

## 3. 第一层：双流 Interval Join + turn 证据聚合

### 3.1 Join

```
botReply.keyBy(turn_id)
  .intervalJoin(spans.keyBy(turn_id))
  .between(Duration.ofSeconds(-30), Duration.ofMinutes(2))
  .sideOutputRightLateData(LATE_SPAN)
  .process(new TurnSpanJoinFunction())
```

- `TurnSpanJoinFunction` 做两件轻量过滤：span 必须属于同一 `session_id`
  （防 turn_id 碰撞/复用）；按 `span_id` 去重（重复上报只计一次）。
- 一对多不直接展开成多行进入 CEP（否则一次问答在 CEP 里变成多个重复事件）。
  Join 输出进入下一步折叠。

### 3.2 证据聚合

`keyBy(turn_id)` 的 `KeyedProcessFunction`：

- 状态：`replyState`（一条 bot_reply）、`spanListState`（该 turn 的 span）。
- 收到 reply 时注册事件时间定时器 `reply.event_time + 2min`；
  定时器触发时输出一条 **TurnEvidenceSnapshot**：

```
TurnEvidenceSnapshot {
  session_id, turn_id, biz_line, user_id, intent, reply_time,
  reply_mode,
  recall_hits: Map<channel, hit_count>,
  recall_miss_all: boolean,        // 所有召回路 hit_count=0
  rerank_model_version, top_score,
  fallback: boolean,
  latency: {recall_ms, rerank_ms, rag_ms, total_ms},
  span_complete: boolean           // 2min 内是否等到至少一条 span
}
```

- 没等到任何 span：`span_complete=false`，快照照常下发（CEP 规则把"无证据"
  与"证据正常但质量差"区分开），同时计入埋点覆盖率。
- 替代实现：Join + 聚合也可整体用一个 `KeyedCoProcessFunction`
  （reply/span 两条输入 + 同一定时器）完成。保留 Interval Join 的理由是它
  明确表达了双流时间区间语义，迟到数据有标准侧输出，便于讲清数据边界。

### 3.3 Join 层验收信号（每一跳都要有信号）

| 观测点 | 指标 |
| --- | --- |
| S1/S2 入口 | 每秒条数、脏数据数、当前水位线 |
| Join | 配对成功数、右侧晚到丢弃数、span 重复数、无 span 的 turn 比例 |
| 聚合 | 快照输出数、2min 触发延迟分布 |

---

## 4. 第二层：覆盖生命周期的 CEP 规则

| 规则 | 阶段 | key | 时间语义 | within | 动作 |
| --- | --- | --- | --- | --- | --- |
| R1 首响超时 | 进线应答 | session_id | processing time | 业务线 SLA | 预警/升级 |
| R2 答非所问循环 | 多轮解决 | session_id | event time | 10 min × 3 轮 | 转人工 + badcase |
| R3 agent 转圈 | multi-agent 路由 | session_id | event time | 15 min × 3 次 | 转人工 + 架构指标 |
| R4 灰度版本连续劣化 | 模型/召回 | model_version（+biz_line） | event time | 5 min × N 次 | 灰度告警/回滚建议 |
| R5 短时复联 | 结案后 | user_id | event time | 2 h | 预警 |
| R5' 7 天一次解决率 | 全周期 | user_id+intent | Doris 准实时 | 7 天 | 报表 |

### R1 · 首响超时

不走 CEP（CEP 擅长"序列出现"，"好事没发生"需要超时侧输出，且首响 SLA
是挂钟口径）。在会话流上 keyBy session_id 的进程函数：收到 `session_start`
注册 PT 定时器（CRM/达人 30s，商家自助 60s，阈值来自广播配置），
定时器前收到有效 `bot_reply` 则删除；触发则输出 SLA 告警。
大促期间可对同业务线做窗口二次聚合（超时率 > x% 升级 oncall）。

### R2 · 答非所问循环（核心规则，最能体现 Join+CEP 组合）

模式（形式化）：

```
low_quality_reply: TurnEvidenceSnapshot 满足
    fallback == true
    OR top_score < θ_score(按 biz_line 配置)
    OR recall_miss_all == true
    OR total_ms > θ_latency
followedBy
negative_signal: 用户在该 turn 后短时间内
    点踩 / 重复相同 intent 的 user_message / 明确"没解决"类意图
times(3) .within(10 min)
AfterMatchSkipStrategy.skipPastLastEvent()
```

- 用户负向信号是会话流，需要再与证据流按 turn_id 对齐：负向信号挂在
  对应 turn 上。实现倾向：**证据聚合函数同时监听会话流**，
  在定时器窗口内把该 turn 的点踩/追问并入快照，CEP 只消费一种事件，模式最简单。
- 输出：`RiskAlert(rule=R2, session_id, turns=[...证据明细], model_version,
  rule_version)`，动作 = 实时转人工，落 badcase 队列。
- 阈值 θ 冷启动：先用离线 badcase 标注集回归出默认阈值，
  再通过规则配置灰度调整，不手工改代码。

### R3 · agent 转圈（multi-agent 升级的退化探针）

```
begin(agent_handoff, intent 未变化)
  .followedBy(agent_handoff, intent 未变化)
  .followedBy(agent_handoff, intent 未变化)
  .within(15 min)
```

- "intent 未变化"通过 `IterativeCondition` 比较序列中各 turn 的 intent。
- 输出 agent 路径（A→B→C）、各 agent 停留时长。这条指标在单 agent 时代不存在，
  是 multi-agent 架构升级后必须盯的新退化面，也能直接回答
  "升级后转圈率/平均转接次数是否变差"。

### R4 · 模型/召回路灰度退化（实时灰度门禁）

输入为 span 流（无需等会话闭环，延迟最低），按 `model_version`（或
`model_version + recall_channel`）keyBy：

```
begin(degrade: latency_ms > P99 阈值 OR (span_type=recall AND hit_count=0)
              OR (span_type=rerank AND top_score < θ))
  .timesOrMore(N).consecutive().within(5 min)
```

- "连续 N 次"而不是窗口计数，语义上排除偶发抖动；`consecutive()`
  要求中间不出现正常样本。
- 灰度新版本时对比同业务线基线版本的触发率，作为继续放量/回滚的实时证据。
- 注意 key 基数：版本数是有限的，状态极小；**不要用 session_id 当 key**。

### R5 · 复联与一次解决率（CEP 能力边界的取舍）

- 实时部分：`rating(≤2)` 后 2h 内同用户同 intent `session_start`，
  Interval Join（between 0, 2h，key = user_id+intent）即可，
  配 RocksDB + 状态 TTL 2h。
- 7 天口径：放 Doris。结案评分明细与后续进线明细做区间关联/宽表，
  分钟级或小时级调度。理由：7 天 × 全量用户 × 每会话 N 条候选的 keyed state
  在内存紧张的机器上风险大、恢复慢，收益又只是报表，不值得占用实时状态。

---

## 5. 规则动态化：版本化广播配置（复用现有模式）

照搬 water-flink 已验证的链路（见 `HEAT_RULES.md`）：**MySQL 配置表 → Kafka 配置 topic
→ Flink 广播状态**。

```sql
CREATE TABLE cs_quality_rule (
  version      BIGINT UNSIGNED PRIMARY KEY,
  status       TINYINT UNSIGNED NOT NULL,      -- 0 PENDING / 1 ACTIVE / 2 DISABLED
  biz_line     VARCHAR(32) NOT NULL,           -- 业务线差异化
  rule_code    VARCHAR(32) NOT NULL,           -- R1..R5
  params_json  JSON NOT NULL,                  -- 阈值/次数/窗口
  note         VARCHAR(255),
  published_at DATETIME(3),
  active_slot  TINYINT GENERATED ALWAYS AS (CASE WHEN status=1 THEN 1 END) STORED,
  UNIQUE KEY uk_active (active_slot)
);
```

`params_json` 示例：

```json
{"first_response_timeout_s":30,"loop_times":3,"loop_window_s":600,
 "rerank_min_score":0.42,"max_latency_ms":3000,"handoff_circle":3}
```

工程边界：

- **CEP Pattern 的结构（顺序/times/within）编译期固定**，运行期广播只更新
  阈值条件读取的参数（在条件函数里从广播状态取当前版本）。
  结构变更走发版，避免运行时动态编译 Pattern 的复杂度。
- 版本单调递增；告警记录规则版本，保证"用哪条规则判的"可追溯。
- 配置发布同样要注意 MySQL/Kafka 非原子：Kafka 发送失败要重发同版本完整快照。

---

## 6. 输出、干预与闭环

### 6.1 实时干预流（唯一要求秒级）

- 不建议在 TaskManager 里直接 HTTP 调坐席系统（重试、反压、连接管理都会污染
  算子）。统一写 Kafka `cs.intervention.command.v1`：

```json
{"alert_id":"<确定性幂等键>","rule_code":"R2","biz_line":"talent",
 "session_id":"s1","user_id":"u9","action":"handoff_human",
 "evidence_turn_ids":["t3","t5","t7"],"model_version":"rerank-...-gray2",
 "rule_version":17,"event_time":1792470001234}
```

- `alert_id = hash(rule_code + session_id + 首个匹配事件 id)`，确定性生成，
  作业重放/恢复时下游按 alert_id 去重，保证转人工指令不重复执行。
- 坐席系统执行后把 `intervention_result` 回发会话事件流，
  形成"告警→执行→确认"闭环，干预成功率本身成为指标。

### 6.2 明细与数仓（Doris）

`cs.quality.alerts.v1`（全量告警，含证据 JSON）与
`cs.turn.evidence.v1`（turn 证据快照，可选，量级大时采样/只落低质 turn）
经 Routine Load 入 Doris。分层建议：

| 层 | 表 | 模型 | 说明 |
| --- | --- | --- | --- |
| ODS | `ods_cs_session_event` / `ods_cs_trace_span` | Duplicate | 原始事件，按天分区 |
| DWD | `dwd_cs_turn_evidence` | Unique(turn_id) | Join+聚合后的轮次宽表 |
| DWD | `dwd_cs_risk_alert` | Unique(alert_id) | 告警明细，幂等覆盖 |
| DWS | `dws_cs_quality_1h` | Aggregate/Unique | biz_line × 模型版本 × 小时：首响超时率、转人工率、转圈率、各段 P99 |
| ADS | `ads_cs_service_funnel` / `ads_cs_model_compare` | | 进线→首响→解决→好评→不复联漏斗；灰度版本对比 |

### 6.3 badcase 回流

R2 命中会话自动入回流队列（Kafka → 标注平台/语料库），携带：
用户问题、召回候选及命中情况、rerank 分数、生成回答、点踩/追问证据。
形成闭环：**线上发现 → 证据归因 → 召回扩充/RAG 语料/精排训练 →
灰度（R4 守门）→ 线上再验证（DWS 对比）**。

---

## 7. 状态清单与内存预算

| 状态 | key | 保留上界 | 量级估算（需用实测 QPS 替换） |
| --- | --- | --- | --- |
| Join 配对缓冲 | turn_id | ±2 min 区间 | 1000 turn/min × 2min ≈ 2k 键/并行实例 |
| 证据聚合 | turn_id | reply + 2 min | 同上，每键 1 reply + 几条 span（KB 级） |
| R2 部分匹配 | session_id | 10 min | 并发会话数决定，单值小 |
| R3 部分匹配 | session_id | 15 min | 同上 |
| R4 部分匹配 | model_version | 5 min | 键 = 版本数，常量级 |
| R5 实时复联 | user_id+intent | 2 h + TTL | 2h 进线用户数，单值小 |

控制原则：

1. **结案即清理**：收到 `session_end` 后主动清理该 session 的所有 keyed state，
   不只等 TTL。
2. 全部状态后端用 **RocksDB（增量 checkpoint）**，与现有 water-flink 一致；
   状态 TTL 显式配置（Join/聚合 10 min、CEP 模式内 within 自带清理、复联 2h）。
3. CEP 规则一律 `skipPastLastEvent`，避免长会话产生组合爆炸的匹配状态。
4. checkpoint 间隔建议 60s（干预流对恢复时间敏感）。

---

## 8. 一致性与时间语义专题

- **端到端语义**：Kafka source → Flink → Kafka sink 可做到 exactly-once
  （checkpoint 两阶段提交）；Doris Routine Load 依赖 alert_id/turn_id
  幂等覆盖。整体按"Kafka 内精确一次 + 下游幂等"设计。
- **水位线拖慢**：Join 算子水位线取两流最小值，span 晚到会推迟 R2/R3 触发。
  因此：① R1 用处理时间不依赖水位线；② 监控各算子 currentInputWatermark
  与 wall clock 的差值（watermark lag），超过阈值告警；
  ③ span 源开启 idleness。
- **乱序**：20s 乱序 + 2min Join 上界；埋点超过上界进侧输出，
  在 Doris 里可做 T+1 回补修正报表，但不改变实时告警。
- **可观测分段**：Source→Join→聚合→CEP→每个 sink 逐段计数
  （输入条数、输出条数、脏数据、侧输出、sink 失败数），
  避免"下游没告警"时分不清是没数据、被过滤、还是写失败。

---

## 9. 部署方案（内存受限环境）

现状：8081 session 集群跑 2 个 Hudi 作业（6 slot，仅剩 1）；
8082 water-flink 专属集群 6 slot 已全部被统计作业占满，TM 12GB。
两个集群的 `lib/` 都已有 `flink-cep-2.0.2.jar`。

**推荐：独立作业 + 独立小 TaskManager，不与统计作业混部。**

- 新增一个 systemd 管理的小 TM（或独立 application 集群），初始 1 slot、
  process memory 2048–3072m、RocksDB、managed memory 512m 左右；
  作业并行度 1–2。先按第 7 节的实测 QPS 校准，状态量为 MB～百 MB 级，单机足够。
- 独立作业的好处：CEP 状态/规则变更不影响已有统计作业的 savepoint 兼容链；
  故障爆炸半径隔离；资源可单独回收。
- 不建议把它塞进当前统计 JAR：会让现有作业的状态拓扑和恢复时长继续膨胀。
  （评论风控分支体量小、共用同一事件源，才选择了内嵌；本场景输入源和
  状态维度都独立，应单独成作业。）
- 压测前先用历史 topic 回放跑一遍（独立消费者组），
  验证 watermark lag、状态增长、告警量级，再切实时。

环境变量沿用现有风格：`KAFKA_BOOTSTRAP_SERVERS`、`RULE_CONFIG_TOPIC`、
`SESSION_TOPIC`、`SPAN_TOPIC`、`JOIN_BOUND_LATE`、`RISK_PARALLELISM` 等，
差异全部收敛到配置层，不硬编码进代码。

---

## 10. 分阶段落地与验收（问题-证据-改动-验证闭环）

1. **P0 契约与埋点体检（不改 Flink）**
   对齐两 topic schema；统计 span 晚到分布、turn_id 覆盖率、重复率。
   验收：覆盖率与晚到 P99 有明确数字；缺失字段清单排期。
2. **P1 Join + 证据快照落 Doris**
   验收：turn 配对率、无 span 比例；Doris `dwd_cs_turn_evidence`
   与抽样会话人工核对一致；各段计数看板齐备。
3. **P2 R1 + R2 + 实时转人工**
   验收：历史回放命中率与人工标注 badcase 的精确率/召回率；
   注入合成事件（循环点踩、超时不回复）端到端验证；
   干预指令幂等性（重放不重复转人工）。
4. **P3 R3 + R4**
   验收：灰度版本注入劣化 span 能触发 R4；agent 转圈合成序列命中 R3；
   multi-agent 升级前后转圈率对比出数。
5. **P4 R5 + ADS + badcase 回流**
   验收：一次解决率与离线口径对齐（误差可解释）；
   R2 告警进入标注队列并在下一版模型的灰度对比中看到指标改善。

每阶段上线记录沿用 `deploy/*_UPGRADE.md` 格式：作业 ID、savepoint、
JAR 哈希、规则版本、回滚步骤、线上验收数字。

---

## 11. 风险与开放问题

- **Q1 埋点 schema 对齐**：span 是否都有 `turn_id`、召回路命名是否统一、
  模型版本是否在每次调用都上报——P0 必须确认，否则 Join 无从谈起。
- **Q2 干预通道**：坐席调度系统是否支持按 `alert_id` 幂等消费、
  是否有执行结果回执（intervention_result）。没有回执则 R2 只能做到"告警"，
  闭环缺最后一环。
- **Q3 阈值冷启动**：θ_score、SLA 秒数初值依赖离线标注，
  上线后两周内预期需要通过广播配置频繁调整。
- **Q4 隐私合规**：证据快照含用户原文与对话内容，落 Doris/回流队列前确认
  脱敏与保留周期要求；必要时只存 hash/截断文本。
- **Q5 量级**：本设计的状态估算基于假设 QPS，P0 阶段必须用真实峰值替换，
  再决定并行度与 TM 规格。

---

## 12. 一页讲述线（面试/汇报用）

一通用户进线：R1 盯首响；每一轮问答，Interval Join 把"用户看到的回复"
和"召回/精排/RAG 的内部 span"对齐成证据快照；R2 在证据+点踩序列上识别
答非所问循环并实时转人工；R3 盯 multi-agent 升级后特有的转接兜圈子；
R4 在模型版本维度把连续劣化挡在灰度期；结案后 R5 用实时 Join + Doris
长周期口径看一次解决率。规则走版本化广播，阈值调整不重启；状态全部
有 TTL、结案即清理、RocksDB 承载；告警确定性幂等键保证干预不重复；
所有证据入 Doris 分层并回流 badcase，把 RAG、精排、召回、multi-agent
的每一次优化纳入同一个"发现→归因→优化→灰度→验证"闭环。
