package com.github.xingjianluo.metricsalertenginejava.engine;

/**
 * 引擎容量与降级配置。
 */
public final class EngineConfig {

    private final int maxMetrics;
    private final int maxSeriesPerMetric;
    private final int maxPointsPerSeries;
    private final int maxRules;
    private final int evalQueueCapacity;
    private final long defaultRetentionMillis;
    private final long defaultAllowedFutureMillis;

    public EngineConfig(int maxMetrics, int maxSeriesPerMetric, int maxPointsPerSeries, int maxRules,
                        int evalQueueCapacity, long defaultRetentionMillis, long defaultAllowedFutureMillis) {
        this.maxMetrics = maxMetrics;
        this.maxSeriesPerMetric = maxSeriesPerMetric;
        this.maxPointsPerSeries = maxPointsPerSeries;
        this.maxRules = maxRules;
        this.evalQueueCapacity = evalQueueCapacity;
        this.defaultRetentionMillis = defaultRetentionMillis;
        this.defaultAllowedFutureMillis = defaultAllowedFutureMillis;
    }

    public static EngineConfig defaults() {
        // TODO 骨架
        return null;
    }

    public int maxMetrics() {
        return maxMetrics;
    }

    public int maxSeriesPerMetric() {
        return maxSeriesPerMetric;
    }

    public int maxPointsPerSeries() {
        return maxPointsPerSeries;
    }

    public int maxRules() {
        return maxRules;
    }

    public int evalQueueCapacity() {
        return evalQueueCapacity;
    }

    public long defaultRetentionMillis() {
        return defaultRetentionMillis;
    }

    public long defaultAllowedFutureMillis() {
        return defaultAllowedFutureMillis;
    }
}
