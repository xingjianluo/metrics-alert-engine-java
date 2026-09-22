package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.Collections;

/**
 * 指标定义（版本化）。名称、允许的标签键集合、单指标基数上限在创建时确定；
 * 定义更新（如放宽标签键）会产生新版本，历史样本仍归属写入时的版本，可按版本复现窗口结论。
 */
public final class MetricDefinition {

    private final String name;
    private final int version;
    private final Set<String> allowedLabelKeys;
    private final int maxSeriesCardinality;
    private final long minTimestampMillis;
    private final long maxTimestampMillis;

    public MetricDefinition(String name, int version, Set<String> allowedLabelKeys,
                            int maxSeriesCardinality, long minTimestampMillis, long maxTimestampMillis) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("metric.name.empty: metric name must not be blank");
        }
        if (name.contains(LabelSet.SEPARATOR)) {
            throw new IllegalArgumentException(
                    "metric.name.separator: metric name must not contain '" + LabelSet.SEPARATOR + "': " + name);
        }
        if (maxSeriesCardinality <= 0) {
            throw new IllegalArgumentException("metric.cardinality.invalid: cardinality must be positive: "
                    + maxSeriesCardinality);
        }
        if (minTimestampMillis > maxTimestampMillis) {
            throw new IllegalArgumentException(
                    "metric.timerange.invalid: minTimestamp > maxTimestamp for metric " + name);
        }
        this.name = name;
        this.version = version;
        this.allowedLabelKeys = allowedLabelKeys == null ? Set.of()
                : Collections.unmodifiableSet(new TreeSet<>(allowedLabelKeys));
        this.maxSeriesCardinality = maxSeriesCardinality;
        this.minTimestampMillis = minTimestampMillis;
        this.maxTimestampMillis = maxTimestampMillis;
    }

    public String name() {
        return name;
    }

    public int version() {
        return version;
    }

    public Set<String> allowedLabelKeys() {
        return allowedLabelKeys;
    }

    public int maxSeriesCardinality() {
        return maxSeriesCardinality;
    }

    public long minTimestampMillis() {
        return minTimestampMillis;
    }

    public long maxTimestampMillis() {
        return maxTimestampMillis;
    }

    /** 基于当前定义的时间戳范围，产生版本号为 newVersion 的新定义。 */
    public MetricDefinition withVersion(int newVersion, Set<String> newKeys, int newCardinality) {
        return new MetricDefinition(name, newVersion, newKeys, newCardinality,
                minTimestampMillis, maxTimestampMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricDefinition that)) {
            return false;
        }
        return version == that.version && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, version);
    }

    @Override
    public String toString() {
        return name + "@v" + version;
    }
}
