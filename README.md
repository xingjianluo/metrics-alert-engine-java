# 本地指标聚合与告警评估引擎（metrics-alert-engine-java）

一套纯内存的、可解释的本地指标聚合与告警评估能力。用内存时间序列模拟采集数据，
保证多维指标在**并发写入、窗口边界、乱序/重复数据、规则变更（版本化）、
指标口径变更（指标定义版本化）**下结论一致、状态迁移确定、丢弃原因可区分、
写入路径不被消费速度拖垮。

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
| `DEFINITION_VALUE_OUT_OF_RANGE` | 指标已登记定义，写入值超出该版本声明的取值范围（含 NaN/无穷大） | 入队前，按定义版本校验 |
| `DEFINITION_LABEL_KEY_NOT_ALLOWED` | 指标已登记定义，写入用到了白名单之外的标签键 | 入队前，按定义版本校验 |
| `DEFINITION_LIMIT_EXCEEDED` | 指标定义数量或单指标定义版本数量超上限 | 登记 / 更新定义时 |

> 后三类是**指标口径层面**的拒绝，与数据格式问题（`INVALID_LABEL`、
> `INVALID_METRIC_NAME`）明确区分：标签键格式非法走 `INVALID_LABEL`，
> 格式合法但不在该指标白名单内走 `DEFINITION_LABEL_KEY_NOT_ALLOWED`。
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

### 1.4 指标定义（MetricDefinition）与版本语义

指标本身只有名字和标签，无法表达“口径”。为此引入**指标定义**，一个版本一条不可变记录：

```
MetricVersion(metric, version), unit（单位）, description（口径说明）,
defaultAggregation（默认聚合口径 SUM/COUNT/AVG/MIN/MAX）,
minValue / maxValue（取值范围，闭区间，可单侧留空）,
allowedLabelKeys（允许出现的标签键白名单，空集=不限制）,
createdAtMillis
```

**版本化（与规则版本化同样的只增不改语义）：**

- 首次登记产生 **v1**；之后每次更新产生单调递增的新版本，
  **旧版本不可变并永久保留**，可按版本号逐条取回当时的单位 / 默认聚合 / 范围 / 白名单。
- 每个指标的版本号分配在 per-metric 锁内完成；并发更新在全局管理锁内串行，
  因而不会出现同号版本或超配。

**与未登记写入的兼容范围（宽松语义不收紧）：**

- **未登记定义的指标完全照旧**：任意有限值、任意格式合法的标签键都正常写入，
  不因为引入这套能力而让原来能写的调用方突然写不进去。其写入结论
  `definitionVersion=null`。
- 已登记定义的指标才启用口径校验；白名单为**空集时同样表示不限制标签键**
  （与未登记一致），此时只按取值范围校验。

**写入校验如何选取版本（并发交错下保持自洽）：**

- 定义版本在**入队前同步快照**：一次写入在进入队列前就固定“当时的最新定义版本”，
  版本号随任务进入队列，校验、落库、返回结论始终依据同一个版本。
  因此定义更新与写入并发交错时，不会出现“按新版校验、按旧版记账”的口径漂移。
- 范围越界（含 `NaN`/无穷大）→ `DEFINITION_VALUE_OUT_OF_RANGE`；
  标签键不在白名单 → `DEFINITION_LABEL_KEY_NOT_ALLOWED`。
  两类拒绝在入队前完成，越界数据绝不进入存储，拒绝计数与接受计数严格守恒。

### 1.5 按指标定义版本复现查询结论

同一批**已经落库的数据只存一份**，但可以用不同版本的定义来解读：

- 查询参数 `defVersion=n`：用第 n 版历史定义的**默认聚合口径**聚合同一批点，
  结论与该版本当时一致；数据没变，口径回放到了旧版本。
- 不指定 `defVersion`：走**当前最新定义版本**的默认聚合口径。
- 指标从未登记定义：必须显式给 `function`（保持宽松模式），结果中
  `definitionVersion=null, definitionRegistered=false`。
- 显式给了 `function` 时覆盖定义默认口径，但返回结果仍标明解析到的定义版本。
- 返回体 `MetricQueryResult` 明确包含：`definitionVersion`（本次实际使用的版本）、
  `definitionRegistered`、`function`（实际生效的算子）与各时间线窗口序列。

> 这解决了“指标口径一改，老窗口结论对不上”的问题：旧版本口径下重放得到历史结论，
> 新版本口径下反映新口径，数据无需迁移或双写。

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
| 指标定义数量 | 超 `max-metric-definitions` 拒绝登记新指标（旧定义不受影响） | `DEFINITION_LIMIT_EXCEEDED`、`definitionsRejected` |
| 单指标定义版本数 | 超 `max-definition-versions-per-metric` 拒绝产生新版本（旧版本全部保留） | `DEFINITION_LIMIT_EXCEEDED`、`definitionsRejected` |

定义容量超限时的策略是**显式拒绝、不静默吞掉**：调用方收到
`MetricDefinitionLimitException`（HTTP 422，body 含 `reason=DEFINITION_LIMIT_EXCEEDED`），
同时 `MetricsRegistry` 累加 `definitionsRejected`。
登记成功累加 `definitionsCreated`，更新成功累加 `definitionsUpdated`，三者都可在
`GET /api/metrics/stats` 看到。版本超限时**既不覆盖旧版本、也不产生半成品新版本**。

所有接受量与各原因拒绝量都由 `MetricsRegistry` 原子计数，可通过
`GET /api/metrics/stats` 查看：`accepted`、`rejected`（按原因拆分）、
`totalSeries`、`totalSamples`、`queueDepth`、
`definitionsCreated`、`definitionsUpdated`、`definitionsRejected`。

设计取向：**过载时优先保护写入路径可用性**——消费慢只会快速拒绝新数据并留下明确计数，
而不会让调用线程阻塞或被拖垮。

---

## 5. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/metrics/write` | 写入一条采样（202 接受；422 拒绝，body 含 `reason/detail/definitionVersion`） |
| `GET  /api/metrics/query?metric&from&to&windowMillis&function&defVersion&labels...` | 窗口聚合查询；`function` 可省略（按定义默认口径），`defVersion` 指定历史定义版本，返回 `MetricQueryResult`（含所用版本） |
| `GET  /api/metrics/stats` | 容量、各类拒绝原因计数与指标定义操作计数 |
| `POST /api/metric-definitions` | 登记指标定义（v1）；重复登记 4xx，容量超限 422 |
| `PUT  /api/metric-definitions` | 更新定义（产生新版本；不存在则登记为 v1）；版本数超限 422 且旧版本保留 |
| `GET  /api/metric-definitions/{metric}/versions` | 列出该指标全部定义版本 |
| `GET  /api/metric-definitions/{metric}/versions/{v}` | 取回指定历史版本定义 |
| `GET  /api/metric-definitions/{metric}/latest` | 取回最新版本定义 |
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

指标定义示例（百分比、默认 AVG、范围 `[0,100]`、只允许 host/region 标签）：

```bash
curl -XPOST localhost:8080/api/metric-definitions -H 'Content-Type: application/json' \
  -d '{"metric":"cpu","unit":"%","description":"cpu usage percent",
       "defaultAggregation":"AVG","minValue":0.0,"maxValue":100.0,
       "allowedLabelKeys":["host","region"]}'

# 口径变更（例如默认改成 MAX、量程放宽到 200）-> 产生 v2，v1 永久保留
curl -XPUT localhost:8080/api/metric-definitions -H 'Content-Type: application/json' \
  -d '{"metric":"cpu","unit":"%","description":"cpu usage percent v2",
       "defaultAggregation":"MAX","minValue":0.0,"maxValue":200.0,
       "allowedLabelKeys":["host","region","az"]}'

# 用 v1 的口径复现历史结论（不指定 defVersion 则走最新版本）
curl 'localhost:8080/api/metrics/query?metric=cpu&from=10000&to=11000&windowMillis=1000&defVersion=1'
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

共 40 个测试，覆盖：

| 测试类 | 场景 |
| --- | --- |
| `LabelsAndStatsTest` | 标签规范串确定性 / 非法标签 / 窗口去重统计 |
| `TimeSeriesStoreTest` | **窗口边界（半开、对齐）**、空窗口标记、乱序与重复、标签隔离、时间戳越界、基数上限 |
| `AlertStateMachineTest` | NORMAL→PENDING→FIRING→RECOVERING→NORMAL、**抖动不重复触发**、重复窗口幂等、**静默只抑制不影响状态** |
| `RuleVersioningTest` | 更新产生新版本、历史结论按版本复现、版本间状态隔离 |
| `IngestionServiceTest` | 入队前拒绝、**队列满降级 QUEUE_FULL 且不阻塞**、**并发写入计数守恒** |
| `ConcurrentEvaluationTest` | **并发求值**每组合每窗口恰好迁移一次、通知不重复 |
| `SnapshotAndRetentionTest` | **并发写查快照自洽**（计数不变量）、保留期淘汰与空时间线清理 |
| `MetricDefinitionTest` | 定义创建/更新版本化、**旧版本永久可取回**、非法 Spec 不入库、**指标数/版本数上限显式拒绝并计数** |
| `DefinitionIngestionValidationTest` | **值域越界 / 标签键越界被可区分拒绝**、未登记指标宽松兼容、空白名单不限制、边界值合法、口径更新后同值结论翻转、结果带版本号 |
| `DefinitionVersionQueryTest` | **同批数据 v1/v2 口径结论可复现**（AVG 75 vs MAX 90）、不指定版本走最新、未登记需显式算子、显式算子覆盖但仍报版本 |
| `DefinitionConcurrencyTest` | **定义更新与写入交错下计数守恒**（总判定=接受+拒绝=落库点数）、拒绝只来自定义口径、并发更新版本号唯一连续、并发查询版本与算子始终自洽 |

断言要点：**计数守恒**（accepted = 落库点数；有效迁移数 = 组合数 × 窗口数；
定义交错下 总写入 = accepted + 各类 rejected）、
**状态迁移确定**、**失败原因可区分（数据原因 vs 定义口径原因）**、
**版本可复现（同一批数据在指定历史定义版本下结论稳定）**。

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
[定义] 首次登记 cpu -> cpu#def-v1 口径=AVG 范围=[0.0,100.0] 标签白名单=[host, region]
[输入] 同一批落库数据 60.0,90.0；v1 默认口径 AVG，v2 默认口径 MAX
[判定] v1 结论 value=75.0 版本=cpu#def-v1；v2 结论 value=90.0 版本=cpu#def-v2；不指定版本->cpu#def-v2
[判定] value=100.01 被拒绝 reason=DEFINITION_VALUE_OUT_OF_RANGE（区别于 INVALID_* 数据问题）
[守恒] accepted(registry)==结论接受==落库点数；总判定 == accepted + 各原因 rejected（定义更新交错，不重不漏）
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
          SeriesAggregation, WriteResult, RejectReason,
          MetricDefinition(+Spec), MetricVersion, MetricQueryResult
store/    TimeSeriesStore（时间线/点/窗口聚合/快照）, RuleStore（规则版本化）,
          MetricDefinitionStore（指标定义版本化）,
          AlertStateStore（per-key 锁状态机 + 追加历史）,
          NotificationLog（发出/抑制留痕）, MetricsRegistry（可观测计数）
engine/   IngestionService（有界队列+单worker+降级+定义口径校验）, QueryService（支持按定义版本解读）,
          AlertEvaluator（确定性状态机）, NotificationService（去重+静默）,
          MetricDefinitionService（定义登记/更新/容量策略）,
          MetricDefinitionLimitException（定义容量显式拒绝）,
          RetentionService, EngineClock
config/   EngineProperties, EngineConfig（Spring 装配）
web/      MetricController（写入/版本化查询/stats）, MetricDefinitionController（定义管理）,
          RuleController
```
