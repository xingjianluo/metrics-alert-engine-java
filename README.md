# 本地指标聚合与告警评估引擎（metrics-alert-engine-java）

一套纯内存的、可解释的本地指标聚合与告警评估能力。用内存时间序列模拟采集数据，
保证多维指标在**并发写入、窗口边界、乱序/重复数据、规则变更（版本化）**下结论一致、
状态迁移确定、丢弃原因可区分、写入路径不被消费速度拖垮。

---

## 1. 指标模型

### 1.1 数据结构

| 概念 | 说明 |
| --- | --- |
| `Sample` | 一条采样：`metric`（指标名）+ `Labels`（标签组合）+ `value` + `timestampMillis` |
| `Labels` | 不可变标签集合；内部按 key 排序，规范串形如 `{host="a",service="x"}` |
| 时间线（series） | 由 `metric + Labels` 唯一确定；不同标签组合是不同时间线，**相互隔离** |
| 点（point） | 时间线内以 `timestamp -> value` 存储，时间戳有序、**按时间戳唯一去重** |

- 标签名规则：`[A-Za-z_][A-Za-z0-9_.]*`；标签值非空、长度 ≤ 256、不含控制字符。
- 指标名规则：`[A-Za-z_][A-Za-z0-9_.]*`。

### 1.2 写入与拒绝原因（不静默丢弃）

所有未成功落库的数据都会得到**可区分的拒绝原因**（`RejectReason`），并计入可观测计数：

| 拒绝原因 | 触发条件 | 判定位置 |
| --- | --- | --- |
| `INVALID_LABEL` | 标签名/值非法 | 入队前同步校验 |
| `INVALID_METRIC_NAME` | 指标名非法 | 入队前同步校验 |
| `TIMESTAMP_OUT_OF_RANGE` | 早于 `now-retention` 或晚于 `now+futureTolerance` | 入队前同步校验 |
| `CARDINALITY_LIMIT` | 超过单指标时间线上限或全局时间线上限 | worker 落库时 |
| `DUPLICATE` | 同一时间线、同一毫秒时间戳重复写入（含乱序重复） | worker 落库时 |
| `QUEUE_FULL` | 写入队列已满（消费过慢），丢弃**新到**数据 | 入队时 |
| `SHUTDOWN` | 服务已关闭仍有写入 | 入队 / worker 排空时 |

> 重复数据不是“静默丢弃”：它以 `DUPLICATE` 显式拒绝，保证计数守恒（同一时间戳只计一次）。

### 1.3 写入链路

```
调用方
  │  submit(metric, labels, value, ts)
  ▼
入队前校验（标签 / 指标名 / 时间戳）──不合法──► 立即返回失败（不占队列、不阻塞）
  │ 合法
  ▼
有界队列 ArrayBlockingQueue（容量 metrics.engine.ingest-queue-capacity）
  │ offer 非阻塞；满 ──► 立即返回 QUEUE_FULL（降级：丢新不丢旧，不反压调用方）
  ▼
单 worker 线程串行落库（基数 / 重复检测因此天然无竞争）
```

单 worker 串行落库 + 有界非阻塞入队，是“写入路径不被消费拖垮”和“写一致性”的核心保证。

---

## 2. 窗口与聚合语义

- **滚动窗口（tumbling）**，窗口按 epoch 整数倍对齐：`windowStart = floor(ts / window) * window`。
- 窗口半开区间 `[windowStart, windowStart + window)`，因此**边界点归入右侧窗口**。
- 支持聚合算子：`SUM / COUNT / AVG / MIN / MAX`。
- **乱序数据**：点按时间戳存放，查询时按窗口聚合，乱序到达不影响窗口归属。
- **空窗口不补零**：没有点的窗口标记为 `missing=true, value=NaN`，与“真实出现的 0 值”严格区分，
  保证告警判定可解释（缺失就是缺失）。
- 查询范围 `[from, to)`，与窗口求交；返回按标签规范串排序，结果确定。
- **快照自洽**：每次查询先对命中时间线的点表做拷贝快照（`TreeMap`），再在快照上聚合；
  并发写入不会让一次查询读到“半个新状态”。

---

## 3. 告警规则与状态机语义

### 3.1 规则（版本化）

`AlertRule` 是不可变记录：

```
ruleId#version, metric, labelSelectors, windowMillis,
function(SUM/COUNT/AVG/MIN/MAX), operator(GT/GE/LT/LE/EQ), threshold,
forThreshold, recoveryThreshold, silenceMillis
```

- `labelSelectors`：选择器中的 key 必须等值匹配才命中，未列出的标签不限制。
- **每次更新（`PUT /api/rules`）产生单调递增的新版本，旧版本不可变且永久保留。**
- 告警状态与求值历史按 `(规则版本, 标签组合)` 隔离保存，只追加、永不改写，
  因此**规则更新后历史结论可按版本逐条复现**。

### 3.2 四态状态机（确定语义）

```
                 连续满足 forThreshold 次          再连续满足一次
NORMAL ───────────────────────────────► PENDING ───────────────► FIRING
  ▲                                       │  ▲                     │  │
  │              连续不满足 recoveryThreshold+1（RECOVERING 后再一次） │  │
  │                                       ▼  │                     │  │
  │                                  连续不满足 recoveryThreshold 次  │  │
  │                                       └──┴─────────────────────┘  │
  │                                                                  ▼
  └──────────────────────────── RECOVERING ◄──────────────────────────┘
              （恢复中一旦再次满足，立即回 FIRING，但不重新通知）
```

关键规则：

1. **触发**：连续满足达到 `forThreshold` → `PENDING`；此后再连续满足一个窗口 → `FIRING`，
   此时发且仅发一次 `FIRING` 通知。`PENDING` 本身不通知（“待定”态）。
2. **恢复**：`FIRING/PENDING` 后连续不满足达到 `recoveryThreshold` → `RECOVERING`；
   此后再不满足一个窗口 → `NORMAL`，发一次 `RECOVERED` 通知。
3. **抖动不重复触发**：`RECOVERING` 中途再次满足，立即回到 `FIRING`，但**不发第二次通知**；
   `FIRING` 中持续满足也不重复通知。
4. **幂等求值**：同一 `(规则版本, 标签, windowStart)` 的重复求值被跳过；乱序（更早窗口）求值被忽略。
   每个状态组合有独立锁（per-key lock），状态“读—迁移—记录”在锁内一次完成，并发下计数守恒。
5. **空窗口算“不满足”**：缺失窗口（`NaN`）不计为阈值命中，会驱动恢复计数，避免“无数据却一直告警”。

### 3.3 通知去重与静默期

- 通知只在 `PENDING→FIRING` 与 `RECOVERING→NORMAL` 两类迁移上产生。
- **去重维度是 `(ruleId, 标签组合)`**：规则更新产生新版本不会对同一组合重复通知；
  抖动重新触发也不重复通知。
- **静默期 `silenceMillis` 只抑制通知，不影响状态记录**：静默期内状态机照常迁移，
  被抑制的通知也写入通知日志（`suppressed=true`）留痕。
- 静默判断只看**最近一次真正发出**的通知，被抑制的通知不延长静默窗口。

---

## 4. 容量保护与降级策略

| 保护点 | 策略 | 可观测信号 |
| --- | --- | --- |
| 单指标时间线数 | 超 `max-series-per-metric` 拒绝新建 | `CARDINALITY_LIMIT` |
| 全局时间线数 | 超 `max-total-series` 拒绝新建 | `CARDINALITY_LIMIT` |
| 单线点数 | 超 `max-points-per-series` 时 FIFO 淘汰最旧点（不拒绝新写） | 总点数 / 淘汰量 |
| 数据年龄 | 早于 `retention` 的点被周期淘汰，空时间线被清理 | `RetentionService` |
| 写入速度 | 有界队列满即丢弃**新到**数据，立即失败，绝不阻塞调用方 | `QUEUE_FULL`、`queueDepth` |
| 时间戳范围 | 过旧 / 过未来拒绝 | `TIMESTAMP_OUT_OF_RANGE` |

所有接受量与各原因拒绝量都由 `MetricsRegistry` 原子计数，可通过
`GET /api/metrics/stats` 查看：`accepted`、`rejected`（按原因拆分）、
`totalSeries`、`totalSamples`、`queueDepth`。

设计取向：**过载时优先保护写入路径可用性**——消费慢只会快速拒绝新数据并留下明确计数，
而不会让调用线程阻塞或被拖垮。

---

## 5. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/metrics/write` | 写入一条采样（202 接受；422 拒绝，body 含 `reason/detail`） |
| `GET  /api/metrics/query?metric&from&to&windowMillis&function&labels...` | 窗口聚合查询 |
| `GET  /api/metrics/stats` | 容量与各类拒绝原因计数 |
| `POST /api/rules` | 注册规则（v1） |
| `PUT  /api/rules` | 更新规则（产生新版本；不存在则注册） |
| `GET  /api/rules/{ruleId}/versions` | 列出全部版本 |
| `POST /api/rules/evaluate?fromWindowStart=` | 对所有最新版本逐窗口求值 |
| `POST /api/rules/{ruleId}/versions/{v}/evaluate?fromWindowStart=` | 对**指定版本**求值（复现历史语义） |
| `GET  /api/rules/{ruleId}/versions/{v}/state` | 该版本当前各标签组合状态 |
| `GET  /api/rules/{ruleId}/versions/{v}/history` | 该版本追加式求值历史（可复现结论） |
| `GET  /api/rules/notifications` | 全部通知（含被静默抑制的） |

写入示例：

```bash
curl -XPOST localhost:8080/api/metrics/write -H 'Content-Type: application/json' \
  -d '{"metric":"cpu","labels":{"host":"a"},"value":92.4}'
```

规则示例：`cpu` 的 1s 窗口 `AVG > 80`，连续 2 次触发、连续 2 次恢复、静默 10s：

```json
{
  "ruleId": "cpu_high",
  "metric": "cpu",
  "labelSelectors": {},
  "windowMillis": 1000,
  "function": "AVG",
  "operator": "GT",
  "threshold": 80.0,
  "forThreshold": 2,
  "recoveryThreshold": 2,
  "silenceMillis": 10000
}
```

---

## 6. 验证方法

### 6.1 运行全部测试

```bash
mvn test
```

共 22 个测试，覆盖：

| 测试类 | 场景 |
| --- | --- |
| `LabelsAndStatsTest` | 标签规范串确定性 / 非法标签 / 窗口去重统计 |
| `TimeSeriesStoreTest` | **窗口边界（半开、对齐）**、空窗口标记、乱序与重复、标签隔离、时间戳越界、基数上限 |
| `AlertStateMachineTest` | NORMAL→PENDING→FIRING→RECOVERING→NORMAL、**抖动不重复触发**、重复窗口幂等、**静默只抑制不影响状态** |
| `RuleVersioningTest` | 更新产生新版本、历史结论按版本复现、版本间状态隔离 |
| `IngestionServiceTest` | 入队前拒绝、**队列满降级 QUEUE_FULL 且不阻塞**、**并发写入计数守恒** |
| `ConcurrentEvaluationTest` | **并发求值**每组合每窗口恰好迁移一次、通知不重复 |
| `SnapshotAndRetentionTest` | **并发写查快照自洽**（计数不变量）、保留期淘汰与空时间线清理 |

断言要点：**计数守恒**（accepted = 落库点数；有效迁移数 = 组合数 × 窗口数）、
**状态迁移确定**、**失败原因可区分**。

### 6.2 查看测试中的“输入与判定依据”日志

测试使用 SLF4J 打印每次输入、窗口值、是否命中、状态迁移、连续计数与判定理由。
运行后可在两处查看：

```bash
# 独立决策日志（所有测试类合并，便于阅读输入→判定链路）
cat target/test-decisions.log

# 每个测试类的 surefire 输出
ls target/surefire-reports/
```

日志片段示例：

```
[求值] window=11000 value=90.0 met=true 迁移 NORMAL->PENDING hits=2 misses=0 依据=forThreshold reached -> PENDING
[求值] window=12000 value=90.0 met=true 迁移 PENDING->FIRING hits=3 misses=0 依据=one more satisfying window after PENDING -> FIRING
[判定] RECOVERING 中抖动满足 -> FIRING 但不重复通知，通知总数=0
[判定] 消费阻塞期间 QUEUE_FULL 次数=15，峰值队列深度=4（容量 4）
```

### 6.3 手动联调

```bash
mvn spring-boot:run
# 然后用第 5 节的 curl 接口写入数据、注册规则、触发求值、查看 state/history/notifications/stats
```

---

## 7. 主要代码结构

```
model/    Labels, Sample, WindowStats, AlertRule, RuleVersion, AlertState,
          AlertEvaluation, Notification, AggregateFunction, ComparisonOperator,
          SeriesAggregation, WriteResult, RejectReason
store/    TimeSeriesStore（时间线/点/窗口聚合/快照）, RuleStore（版本化）,
          AlertStateStore（per-key 锁状态机 + 追加历史）,
          NotificationLog（发出/抑制留痕）, MetricsRegistry（可观测计数）
engine/   IngestionService（有界队列+单worker+降级）, QueryService,
          AlertEvaluator（确定性状态机）, NotificationService（去重+静默）,
          RetentionService, EngineClock
config/   EngineProperties, EngineConfig（Spring 装配）
web/      MetricController, RuleController
```
