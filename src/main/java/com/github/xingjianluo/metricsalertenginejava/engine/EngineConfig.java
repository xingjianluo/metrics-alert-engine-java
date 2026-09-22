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

    /**
     * 默认容量：64 个指标定义、单指标 1000 条序列、单序列 10000 个点、256 条规则、
     * 异步评估队列 2048、默认保留 24 小时、允许未来 60 秒时钟偏移。
     */
    public static EngineConfig defaults() {
        return new EngineConfig(64, 1000, 10_000, 256, 2048,
                24L * 60 * 60 * 1000, 60L * 1000);
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
