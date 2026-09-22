package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;

import java.util.Objects;
import java.util.Set;

/**
 * 告警规则（版本化，不可变）。
 * 语义：对 metric 的窗口聚合值 aggregator 应用 operator 与 threshold；
 * 连续 pendingThreshold 个窗口命中 -> FIRING（触发）；
 * FIRING 后连续 resolveThreshold 个窗口未命中 -> RESOLVED（恢复）。
 * silenceMillis 静默期内抑制重复通知但不影响状态迁移与记录。
 * labelFilter 非空时仅对包含这些标签键的序列求值。
 */
public final class AlertRule {

    /** 比较算子。 */
    public enum Operator {
        GT, GTE, LT, LTE, EQ, NEQ;

        public boolean test(double actual, double threshold) {
            // TODO 骨架
            return false;
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

    public boolean matchesLabels(com.github.xingjianluo.metricsalertenginejava.model.LabelSet labels) {
        // TODO 骨架
        return false;
    }

    public AlertRule withVersion(int newVersion) {
        // TODO 骨架：除版本外全部复制
        return null;
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
        return id + "@v" + version + "(" + metric + " " + aggregator + " " + operator + " " + threshold + ")";
    }
}
