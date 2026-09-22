# 本地指标聚合与告警评估引擎（metrics-alert-engine-java）

一个纯内存、零外部依赖（除 Spring Boot Web 外）的本地指标聚合与告警评估引擎。
用内存中的时间序列模拟采集数据，目标是在**并发写入、窗口边界、乱序/重复数据、规则抖动与规则变更**
下，让聚合与告警结论保持确定、守恒、可解释。

- Java 21 + Spring Boot 3.4，核心能力不依赖 Spring，可直接作为库嵌入。
- 全部状态保存在进程内存，进程重启即清空；适合本地验证、教学与单测演示。

## 1. 指标模型

| 概念 | 类型 | 说明 |
| --- | --- | --- |
| 指标定义 `MetricDefinition` | 版本化 | 名称、允许标签键集合、单指标**序列基数上限**、合法时间戳范围（由保留期+未来时钟偏移推导）。更新产生新版本，历史版本保留。 |
| 标签集 `LabelSet` | 不可变 | 有序 `TreeMap`，确定性序列标识 `|k1=v1|k2=v2|`；`|` 为保留分隔符。同一指标下不同标签组合是**相互隔离**的时间序列。 |
| 样本 `Sample` | record | `metric + labels + value + timestampMillis`。 |
| 序列 `Series` | 内存结构 | 单条序列的点保存在 `TreeMap<Long,Double>`：天然支持乱序插入；**相同时间戳按最后写入覆盖（幂等去重）**，不产生第二个点。 |

### 写入拒绝原因（`RejectReason`，可区分、不静默丢弃）

写入走 `MetricsAlertEngine.write(Sample)`，返回 `WriteResult(accepted, duplicate, reason, message, metricVersion)`。
校验顺序固定，错误消息带稳定前缀，便于定位与断言：

1. `INVALID_METRIC_NAME`：指标名为空；
2. `METRIC_UNKNOWN`：指标未注册；
3. `INVALID_VALUE`：值不是有限数（NaN/Infinity）；
4. `INVALID_LABEL`：标签 key 为空、key/value 含保留分隔符 `|`、使用了指标定义未允许的标签键；
5. `TIMESTAMP_OUT_OF_RANGE`：时间戳超出 `[now-retention, now+futureSkew]`；
6. `CARDINALITY_EXCEEDED`：新标签组合会使该指标序列数超过基数上限（已存在的序列不受影响）。

## 2. 窗口与聚合语义

- **固定对齐滚动窗口（tumbling）**：`windowStart = floor(ts / windowSize) * windowSize`，
  区间半开半闭 `[windowStart, windowStart+windowSize)`。边界点：左闭右开，归属唯一。
- 聚合函数：`SUM / AVG / COUNT / MIN / MAX`。
  - 空窗口：`COUNT=0, SUM=0`，`AVG/MIN/MAX=null`（无观测值不参与比较）。
- 查询在存储的**读锁快照**上进行，返回不可变点集后再聚合；写入持写锁。
  并发读写时，每次查询看到的都是某个自洽时刻（序列集合 + 序列内容一致），同一窗口
  `COUNT` 随时间单调不减，且 `SUM/COUNT` 在同一快照内相互自洽。
- 查询可指定**指标版本号**：结果带 `metricVersion` 标注；历史样本数据不随定义变更。

## 3. 告警状态语义

规则 `AlertRule`（版本化、不可变）：`metric + aggregator + operator(GT/GTE/LT/LTE/EQ/NEQ) + threshold`，
窗口大小 `windowSizeMillis`、连续触发阈值 `pendingThreshold`、连续恢复阈值 `resolveThreshold`、
静默期 `silenceMillis`、可选标签键过滤 `labelFilter`。

状态机按 **(规则版本, 标签组合)** 独立维护，三个状态：

```
RESOLVED ──命中(pendingThreshold==1 时直接 FIRING，否则进入 PENDING)──► PENDING
PENDING  ──连续命中达到 pendingThreshold──► FIRING（触发迁移，产出通知）
PENDING  ──任一窗口未命中：命中计数清零（抖动不会累积触发）
FIRING   ──未命中计数达到 resolveThreshold──► RESOLVED（恢复迁移，产出通知）
FIRING   ──任一窗口再次命中：未命中计数清零（抖动不会错误恢复，也不会重复触发）
```

- **严格按窗口顺序、逐窗口求值**：窗口必须与规则窗口对齐，且必须恰好是上一求值窗口的下一个窗口；
  乱序/重复/跳窗一律抛 `IllegalStateException`（`window.gap.or.out.of.order`）。
  空窗口（无数据）同样驱动计数，不能被略过。
- **去重**：状态机本身保证同一轮 FIRING 只触发一次；通知另有
  `dedupKey = ruleId@version + labels + notifyState` 的发送记忆。
- **静默期**：只抑制通知发送（`NotificationRecord.suppressed=true`，消息带
  `[SUPPRESSED by silence window]`），**状态迁移与求值记录照常发生**。静默锚点为该
  (规则版本, 标签) 状态创建时间与上次同 dedupKey 实际发出时间的较大者。
- **可解释**：每个窗口产出 `EvaluationRecord`（输入值、阈值、是否命中、前后状态、迁移原因），
  `explain()` 可读；每次迁移对应 0/1 条 `NotificationRecord`，迁移数与通知数守恒。
- **版本隔离**：规则更新只产生新版本；v1 与 v2 的状态、求值历史、通知历史互不影响，
  同一历史窗口可分别按 v1/v2 重新求值复现。

## 4. 容量保护与降级策略

| 资源 | 上限（`EngineConfig`，默认值） | 触达策略 | 可观测信号 |
| --- | --- | --- | --- |
| 指标定义数 | `maxMetrics=64` | 注册抛异常 `metric.capacity.exceeded` | 异常消息 |
| 单指标序列数 | `maxSeriesPerMetric=1000`（定义可单独覆盖） | 写入拒绝，不创建序列 | `WriteResult.reason=CARDINALITY_EXCEEDED` |
| 规则数 | `maxRules=256` | 注册抛异常 `rule.capacity.exceeded` | 异常消息 |
| 单序列点数 | `maxPointsPerSeries=10000` | **淘汰最旧点**（一次写入最多淘汰一个） | `DropStats[RETENTION_TRIM]` + 最近证据样例 |
| 异步评估队列 | `evalQueueCapacity=2048` | `offer` **立即失败、绝不阻塞写入线程** | `DropStats[EVAL_QUEUE_FULL]` + 证据样例 |

- `DropStats` 按 `DropReason` 分桶计数并保留最近一条证据（指标、标签、窗口起点），
  可通过 `engine.dropStats()` 或 `GET /api/drops` 观测。
- 异步队列只是“消费过慢时降级”的通道：丢弃的只是**评估任务**，样本数据已同步落库；
  任何时候都可用同步 `evaluateWindow/evaluateRange/evaluateThrough` 追平，结论一致。
- 写入路径只有同步校验与有界内存写入，全部失败路径快速返回，不做无限缓冲。

## 5. 主要 API（引擎门面 `MetricsAlertEngine`）

```java
EngineConfig config = EngineConfig.defaults();
MetricsAlertEngine engine = MetricsAlertEngine.create(config); // 系统时钟
// 测试可注入可控时钟：new MetricsAlertEngine(config, () -> mockNowMillis)

engine.registerMetric("cpu", Set.of("host"), 1000, Duration.ofHours(24).toMillis());
WriteResult r = engine.write(new Sample("cpu", LabelSet.of(Map.of("host","a")), 0.9, ts));

WindowAggregate w = engine.queryWindow("cpu", labels, windowStart, 60_000, null,
        List.of(Aggregator.SUM, Aggregator.AVG));

engine.registerRule("cpu-high","cpu", Aggregator.SUM, AlertRule.Operator.GT, 10.0,
        3, 2, 60_000, 5*60_000, Set.of());
List<EvaluationRecord> recs = engine.evaluateRange("cpu-high", labels,
        fromWindowStart, toWindowStartExclusive, null, null);
engine.updateRule("cpu-high", GT, SUM, 100.0, 3, 2, 5*60_000, Set.of()); // -> v2
List<EvaluationRecord> v1 = engine.evaluationHistory("cpu-high", 1, labels); // 按版本复现
```

### 演示用 HTTP 接口（`MetricsController`）

| 方法与路径 | 作用 |
| --- | --- |
| `POST /api/metrics` | 注册指标 |
| `POST /api/metrics/{name}/write` | 写入样本；拒绝返回 **422** 与可区分 `reason` |
| `GET  /api/metrics/{name}/query?windowStart&windowSize&label.host=a` | 窗口聚合查询 |
| `POST /api/rules` | 注册规则 |
| `POST /api/rules/{id}/evaluate` | 同步评估连续窗口 |
| `GET  /api/rules/{id}/state?label.host=a` | 当前状态、求值历史、通知历史 |
| `GET  /api/drops` | 丢弃计数与异步队列深度 |

## 6. 验证方法

```bash
mvn test            # 全部 25 个单测
mvn test -Dtest=ConcurrentWriteTest          # 只跑并发
mvn test -Dtest=AlertStateMachineTest        # 只跑状态机
mvn spring-boot:run                          # 启动演示服务（默认 8080）
```

测试（位于 `src/test/.../tests/`）覆盖：

| 测试类 | 覆盖场景 |
| --- | --- |
| `WriteValidationTest` | 各类非法数据的可区分拒绝原因、基数守恒、重复时间戳幂等覆盖 |
| `WindowBoundaryTest` | 半开窗口边界、epoch 对齐、乱序写入聚合确定性、标签隔离、空窗口 |
| `AlertStateMachineTest` | PENDING/FIRING/RESOLVED 全生命周期、两类抖动不重复触发/不错误恢复、静默期只抑制通知、乱序与跳窗拒绝 |
| `VersioningTest` | 指标定义与规则阈值升级后按历史版本复现结论、版本间状态隔离 |
| `ConcurrentWriteTest` | 8 线程并发同时间戳写入（唯一点守恒、重复计数）、并发基数竞争严格不超限、200 轮并发查询快照自洽 |
| `CapacityDegradationTest` | 单序列保留淘汰（RETENTION_TRIM）、评估队列满快速失败不阻塞写入、指标/规则硬限 |
| `AsyncEvaluationTest` | 慢消费者端到端：队列满丢弃计数、已入队任务全部处理、补齐评估后状态与同步一致 |
| `WebContractTest` | HTTP 契约：注册/422 拒绝/`label.` 前缀标签查询隔离/丢弃观测接口 |

单测日志（logger 名 `ALERT-TEST`，测试时默认输出到控制台）逐条打印**输入样本/窗口**与
**判定依据**（input、threshold、hit、状态迁移原因、通知是否被静默），例如：

```
[lifecycle] JUDGE  --- rule=cpu-high@v1 labels={host=a} window=1789999980000 input=11.0 threshold=10.0 hit=true state PENDING->FIRING (consecutive hits 3 reached pending threshold 3)
[lifecycle] NOTIFY *** FIRED rule=cpu-high@v1 ... condition=[SUM GT 10.0] because: ...
[retention] 写入 8 点 / 容量 5，RETENTION_TRIM=3，证据=metric=cpu labels={host=a} trimmedTs=...
```

## 7. 关键设计取舍

- **时间戳语义**：样本时间戳由数据携带（而非写入墙钟），引擎时钟只用于确定合法时间范围与静默期，
  使窗口计算与历史复现完全确定、可重放。
- **状态机不自动调度**：评估由显式调用驱动（同步或异步队列），窗口推进顺序成为可验证的不变量；
  生产化时可由定时调度器以相同的 `evaluateThrough` 语义接入。
- **聚合快照与求值分离**：求值器只接收不可变 `WindowAggregate`，存储并发模型不会泄漏进状态机。
