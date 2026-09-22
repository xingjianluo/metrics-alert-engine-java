package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.DropReason;
import com.github.xingjianluo.metricsalertenginejava.model.DropStats;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WindowAggregate;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.rule.AlertEvaluator;
import com.github.xingjianluo.metricsalertenginejava.rule.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.rule.EvaluationRecord;
import com.github.xingjianluo.metricsalertenginejava.rule.NotificationRecord;
import com.github.xingjianluo.metricsalertenginejava.rule.RuleRegistry;
import com.github.xingjianluo.metricsalertenginejava.storage.MetricRegistry;
import com.github.xingjianluo.metricsalertenginejava.storage.Series;
import com.github.xingjianluo.metricsalertenginejava.storage.TimeSeriesStore;
import com.github.xingjianluo.metricsalertenginejava.window.WindowAggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.LongSupplier;

/**
 * 引擎门面：组合指标注册表、时序存储、规则注册表、告警求值器与有界异步评估队列。
 *
 * 容量与降级策略：
 * <ul>
 *   <li>写入路径只做同步校验与内存写入（有界、快速失败），任何容量问题都以明确原因返回或计数，
 *       不做无限缓冲；</li>
 *   <li>异步评估队列满时 {@link #offerAsyncEval} 立即丢弃并计数 EVAL_QUEUE_FULL，绝不阻塞写入；
 *       调用方仍可随时用同步 evaluate* 方法追平评估，结论一致；</li>
 *   <li>单序列超容量按最旧淘汰（RETENTION_TRIM）；序列/指标/规则超容量分别以
 *       CARDINALITY_EXCEEDED 拒绝、注册表异常拒绝。</li>
 * </ul>
 */
public final class MetricsAlertEngine {

    private final EngineConfig config;
    private final MetricRegistry metricRegistry;
    private final TimeSeriesStore store;
    private final RuleRegistry ruleRegistry;
    private final AlertEvaluator evaluator;
    private final DropStats dropStats;
    private final LongSupplier clock;
    private final LinkedBlockingQueue<EvalTask> evalQueue;

    public MetricsAlertEngine(EngineConfig config, LongSupplier clock) {
        this.config = config;
        this.clock = clock;
        this.dropStats = new DropStats();
        this.metricRegistry = new MetricRegistry(config.maxMetrics());
        this.store = new TimeSeriesStore(config.maxPointsPerSeries(), dropStats);
        this.ruleRegistry = new RuleRegistry(config.maxRules());
        this.evaluator = new AlertEvaluator(clock);
        this.evalQueue = new LinkedBlockingQueue<>(config.evalQueueCapacity());
    }

    public static MetricsAlertEngine create(EngineConfig config) {
        return new MetricsAlertEngine(config, System::currentTimeMillis);
    }

    // ---- 指标管理 ----

    /** 注册指标，时间合法范围为 [now-retention, now+futureSkew]。 */
    public MetricDefinition registerMetric(String name, Set<String> allowedLabelKeys, int cardinality,
                                           long retentionMillis) {
        long now = clock.getAsLong();
        return metricRegistry.register(name, allowedLabelKeys, cardinality,
                now - retentionMillis, now + config.defaultAllowedFutureMillis());
    }

    public MetricDefinition registerMetric(String name, Set<String> allowedLabelKeys, int cardinality) {
        return registerMetric(name, allowedLabelKeys, cardinality, config.defaultRetentionMillis());
    }

    public MetricDefinition updateMetric(String name, Set<String> allowedLabelKeys, int cardinality) {
        return metricRegistry.update(name, allowedLabelKeys, cardinality);
    }

    public MetricDefinition metric(String name) {
        return metricRegistry.current(name);
    }

    // ---- 写入 ----

    public WriteResult write(Sample sample) {
        if (sample.metric() == null || sample.metric().isBlank()) {
            return WriteResult.rejected(RejectReason.INVALID_METRIC_NAME,
                    "metric.name.empty: sample metric name is null or blank", -1);
        }
        MetricDefinition def = metricRegistry.current(sample.metric());
        if (def == null) {
            return WriteResult.rejected(RejectReason.METRIC_UNKNOWN,
                    "metric.unknown: no metric registered with name " + sample.metric(), -1);
        }
        return store.write(sample, def);
    }

    // ---- 窗口查询 ----

    /** 查询单个窗口的聚合。metricVersion 为 null 时以当前版本标注结果。 */
    public WindowAggregate queryWindow(String metric, LabelSet labels, long windowStart, long windowSize,
                                       Integer metricVersion, List<Aggregator> functions) {
        MetricDefinition def = metricRegistry.version(metric, metricVersion);
        if (def == null) {
            throw new IllegalArgumentException("metric.unknown: " + metric
                    + (metricVersion == null ? "" : " v" + metricVersion));
        }
        List<Aggregator> wanted = functions == null || functions.isEmpty()
                ? List.of(Aggregator.values()) : functions;
        List<Series.Point> points = store.snapshotWindow(metric, labels, windowStart, windowSize);
        return WindowAggregator.aggregate(metric, labels, windowStart, windowSize, def.version(),
                points, wanted);
    }

    /** 查询连续多个窗口 [fromWindowStart, toWindowStartExclusive)，按窗口升序。 */
    public List<WindowAggregate> queryRange(String metric, LabelSet labels, long fromWindowStart,
                                            long toWindowStartExclusive, long windowSize,
                                            Integer metricVersion, List<Aggregator> functions) {
        if (toWindowStartExclusive < fromWindowStart) {
            throw new IllegalArgumentException("window.range.invalid: to < from");
        }
        if ((toWindowStartExclusive - fromWindowStart) % windowSize != 0
                || fromWindowStart % windowSize != 0) {
            throw new IllegalArgumentException("window.range.misaligned: endpoints must align to window size");
        }
        List<WindowAggregate> out = new ArrayList<>();
        for (long w = fromWindowStart; w < toWindowStartExclusive; w += windowSize) {
            out.add(queryWindow(metric, labels, w, windowSize, metricVersion, functions));
        }
        return List.copyOf(out);
    }

    // ---- 规则管理 ----

    public AlertRule registerRule(String id, String metric, Aggregator aggregator, AlertRule.Operator operator,
                                  double threshold, int pendingThreshold, int resolveThreshold,
                                  long windowSizeMillis, long silenceMillis, Set<String> labelFilter) {
        return ruleRegistry.register(new AlertRule(id, 1, metric, aggregator, operator, threshold,
                pendingThreshold, resolveThreshold, windowSizeMillis, silenceMillis, labelFilter));
    }

    public AlertRule updateRule(String id, AlertRule.Operator operator, Aggregator aggregator, double threshold,
                                int pendingThreshold, int resolveThreshold, long silenceMillis,
                                Set<String> labelFilter) {
        return ruleRegistry.update(id, operator, aggregator, threshold, pendingThreshold, resolveThreshold,
                silenceMillis, labelFilter);
    }

    // ---- 告警求值（同步、按窗口确定性推进）----

    /**
     * 评估单个窗口。ruleVersion/metricVersion 为 null 时取当前版本；
     * 窗口必须与规则窗口大小对齐。
     */
    public EvaluationRecord evaluateWindow(String ruleId, LabelSet labels, long windowStart,
                                           Integer ruleVersion, Integer metricVersion) {
        AlertRule rule = ruleRegistry.version(ruleId, ruleVersion);
        if (rule == null) {
            throw new IllegalArgumentException("rule.unknown: " + ruleId
                    + (ruleVersion == null ? "" : " v" + ruleVersion));
        }
        WindowAggregate aggregate = queryWindow(rule.metric(), labels, windowStart, rule.windowSizeMillis(),
                metricVersion, List.of(rule.aggregator()));
        return evaluator.evaluate(rule, labels, aggregate, windowStart).record();
    }

    /** 顺序评估连续窗口 [fromWindowStart, toWindowStartExclusive)。 */
    public List<EvaluationRecord> evaluateRange(String ruleId, LabelSet labels, long fromWindowStart,
                                                long toWindowStartExclusive, Integer ruleVersion,
                                                Integer metricVersion) {
        AlertRule rule = ruleRegistry.version(ruleId, ruleVersion);
        if (rule == null) {
            throw new IllegalArgumentException("rule.unknown: " + ruleId);
        }
        List<EvaluationRecord> out = new ArrayList<>();
        for (long w = fromWindowStart; w < toWindowStartExclusive; w += rule.windowSizeMillis()) {
            out.add(evaluateWindow(ruleId, labels, w, rule.version(), metricVersion));
        }
        return List.copyOf(out);
    }

    /** 评估 [上次评估窗口的下一窗口, timestamp 所属窗口] 之间的全部窗口（首次从时间戳同窗口开始）。 */
    public List<EvaluationRecord> evaluateThrough(String ruleId, LabelSet labels, long timestampMillis,
                                                  Integer ruleVersion) {
        AlertRule rule = ruleRegistry.version(ruleId, ruleVersion);
        if (rule == null) {
            throw new IllegalArgumentException("rule.unknown: " + ruleId);
        }
        long size = rule.windowSizeMillis();
        long target = WindowAggregator.windowStart(timestampMillis, size);
        Long last = evaluator.lastEvaluatedWindows()
                .get(rule.id() + "@v" + rule.version() + labels.seriesKey());
        long from = (last == null) ? target : last + size;
        if (from > target) {
            return List.of();
        }
        return evaluateRange(ruleId, labels, from, target + size, rule.version(), null);
    }

    // ---- 异步评估（容量/降级演示；不影响同步结论）----

    /** 非阻塞入队，队列满立即丢弃并计数 EVAL_QUEUE_FULL；返回是否入队成功。 */
    public boolean offerAsyncEval(EvalTask task) {
        boolean offered = evalQueue.offer(task);
        if (!offered) {
            dropStats.record(DropReason.EVAL_QUEUE_FULL,
                    "rule=" + task.rule().id() + " labels=" + task.labels() + " toWindow="
                            + task.toWindowStart());
        }
        return offered;
    }

    public EvalTask pollAsyncEval() {
        return evalQueue.poll();
    }

    public int evalQueueSize() {
        return evalQueue.size();
    }

    // ---- 历史与状态 ----

    public List<EvaluationRecord> evaluationHistory(String ruleId, Integer version, LabelSet labels) {
        return evaluator.evaluationHistory(ruleId, version, labels);
    }

    public List<NotificationRecord> notificationHistory(String ruleId, Integer version, LabelSet labels) {
        return evaluator.notificationHistory(ruleId, version, labels);
    }

    public AlertState alertState(String ruleId, Integer version, LabelSet labels) {
        AlertRule rule = ruleRegistry.version(ruleId, version);
        if (rule == null) {
            throw new IllegalArgumentException("rule.unknown: " + ruleId);
        }
        return evaluator.currentState(ruleId, rule.version(), labels);
    }

    public DropStats dropStats() {
        return dropStats;
    }

    public int seriesCount(String metric) {
        return store.seriesCount(metric);
    }

    // ---- 组件暴露（供测试与控制器使用）----

    public MetricRegistry metricRegistry() {
        return metricRegistry;
    }

    public RuleRegistry ruleRegistry() {
        return ruleRegistry;
    }

    public AlertEvaluator evaluator() {
        return evaluator;
    }

    public TimeSeriesStore store() {
        return store;
    }

    public EngineConfig config() {
        return config;
    }
}
