package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;

import java.util.Objects;
import java.util.Set;

/**
 * 告警规则（版本化，不可变）。
 * 语义：对 metric 的窗口聚合值 aggregator 应用 operator 与 threshold；
 * 连续 pendingThreshold 个窗口命中 -> FIRING（触发）；
 * FIRING 后连续 resolveThreshold 个窗口未命中 -> RESOLVED（恢复）；
 * silenceMillis 静默期内抑制重复通知但不影响状态迁移与记录；
 * labelFilter 非空时仅对包含全部这些标签键的序列求值。
 */
public final class AlertRule {

    /** 比较算子。AVG/MIN/MAX 在空窗口下为 null，任何比较都判定为“未命中”。 */
    public enum Operator {
        GT, GTE, LT, LTE, EQ, NEQ;

        public boolean test(double actual, double threshold) {
            return switch (this) {
                case GT -> actual > threshold;
                case GTE -> actual >= threshold;
                case LTE -> actual <= threshold;
                case LT -> actual < threshold;
                case EQ -> Double.compare(actual, threshold) == 0;
                case NEQ -> Double.compare(actual, threshold) != 0;
            };
        }
    }

    private final String id;
    private final int version;
    private final String metric;
    private final Aggregator aggregator;
    private final Operator operator;
    private final double threshold;
    private final int pendingThreshold;
    private final int resolveThreshold;
    private final long windowSizeMillis;
    private final long silenceMillis;
    private final Set<String> labelFilter;

    public AlertRule(String id, int version, String metric, Aggregator aggregator, Operator operator,
                     double threshold, int pendingThreshold, int resolveThreshold, long windowSizeMillis,
                     long silenceMillis, Set<String> labelFilter) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("rule.id.empty: rule id must not be blank");
        }
        if (metric == null || metric.isBlank()) {
            throw new IllegalArgumentException("rule.metric.empty: metric must not be blank for rule " + id);
        }
        if (aggregator == null || operator == null) {
            throw new IllegalArgumentException("rule.operator.null: aggregator/operator required for rule " + id);
        }
        if (pendingThreshold < 1 || resolveThreshold < 1) {
            throw new IllegalArgumentException(
                    "rule.threshold.invalid: pending/resolve thresholds must be >= 1 for rule " + id);
        }
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("rule.window.invalid: window size must be positive for rule " + id);
        }
        if (silenceMillis < 0) {
            throw new IllegalArgumentException("rule.silence.invalid: silence must be >= 0 for rule " + id);
        }
        this.id = id;
        this.version = version;
        this.metric = metric;
        this.aggregator = aggregator;
        this.operator = operator;
        this.threshold = threshold;
        this.pendingThreshold = pendingThreshold;
        this.resolveThreshold = resolveThreshold;
        this.windowSizeMillis = windowSizeMillis;
        this.silenceMillis = silenceMillis;
        this.labelFilter = labelFilter == null ? Set.of() : Set.copyOf(labelFilter);
    }

    public String id() {
        return id;
    }

    public int version() {
        return version;
    }

    public String metric() {
        return metric;
    }

    public Aggregator aggregator() {
        return aggregator;
    }

    public Operator operator() {
        return operator;
    }

    public double threshold() {
        return threshold;
    }

    public int pendingThreshold() {
        return pendingThreshold;
    }

    public int resolveThreshold() {
        return resolveThreshold;
    }

    public long windowSizeMillis() {
        return windowSizeMillis;
    }

    public long silenceMillis() {
        return silenceMillis;
    }

    public Set<String> labelFilter() {
        return labelFilter;
    }

    /** 标签集合必须包含 labelFilter 中的全部键（值不限）。 */
    public boolean matchesLabels(LabelSet labels) {
        return labels.labels().keySet().containsAll(labelFilter);
    }

    /** 用新版本号复制规则体（其余字段不变）。 */
    public AlertRule withVersion(int newVersion) {
        return new AlertRule(id, newVersion, metric, aggregator, operator, threshold, pendingThreshold,
                resolveThreshold, windowSizeMillis, silenceMillis, labelFilter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AlertRule alertRule)) {
            return false;
        }
        return version == alertRule.version && Objects.equals(id, alertRule.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version);
    }

    @Override
    public String toString() {
        return id + "@v" + version + "(" + metric + " " + aggregator + " " + operator + " " + threshold
                + ", pending=" + pendingThreshold + ", resolve=" + resolveThreshold + ")";
    }
}
