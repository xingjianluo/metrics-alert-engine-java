package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 指标定义版本标识：同一指标名（metric）每次更新定义产生新版本，
 * 旧版本不可变并永久保留，聚合结论可按定义版本复现。
 *
 * @param metric  指标名
 * @param version 该指标的定义版本号，从 1 开始单调递增
 */
public record MetricVersion(String metric, int version) implements Comparable<MetricVersion> {

    public MetricVersion {
        if (metric == null || metric.isBlank()) {
            throw new IllegalArgumentException("metric must not be blank");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
    }

    public MetricVersion next() {
        return new MetricVersion(metric, version + 1);
    }

    @Override
    public int compareTo(MetricVersion o) {
        int c = metric.compareTo(o.metric);
        return c != 0 ? c : Integer.compare(version, o.version);
    }

    @Override
    public String toString() {
        return metric + "#def-v" + version;
    }
}
