package com.github.xingjianluo.metricsalertenginejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 引擎容量与语义配置（前缀 metrics.engine）。
 */
@ConfigurationProperties(prefix = "metrics.engine")
public class EngineProperties {

    /** 单指标允许的标签组合（时间线）数量上限。 */
    private int maxSeriesPerMetric = 1000;
    /** 全局时间线数量上限。 */
    private int maxTotalSeries = 10_000;
    /** 单条时间线保留的最大点数（FIFO 淘汰）。 */
    private int maxPointsPerSeries = 20_000;
    /** 数据保留时长（毫秒），更早的数据被淘汰。 */
    private long retentionMillis = 24 * 3600_000L;
    /** 允许的未来时间戳容差（毫秒）。 */
    private long futureToleranceMillis = 60_000L;
    /** 写入缓冲队列容量；满时丢弃新写入并记 QUEUE_FULL（保护写入路径不被拖垮）。 */
    private int ingestQueueCapacity = 10_000;
    /** 是否在新时间线首次出现时拒绝（基数保护开关由存储侧使用）。 */
    private long evictionIntervalMillis = 300_000L;

    public int getMaxSeriesPerMetric() { return maxSeriesPerMetric; }
    public void setMaxSeriesPerMetric(int v) { this.maxSeriesPerMetric = v; }
    public int getMaxTotalSeries() { return maxTotalSeries; }
    public void setMaxTotalSeries(int v) { this.maxTotalSeries = v; }
    public int getMaxPointsPerSeries() { return maxPointsPerSeries; }
    public void setMaxPointsPerSeries(int v) { this.maxPointsPerSeries = v; }
    public long getRetentionMillis() { return retentionMillis; }
    public void setRetentionMillis(long v) { this.retentionMillis = v; }
    public long getFutureToleranceMillis() { return futureToleranceMillis; }
    public void setFutureToleranceMillis(long v) { this.futureToleranceMillis = v; }
    public int getIngestQueueCapacity() { return ingestQueueCapacity; }
    public void setIngestQueueCapacity(int v) { this.ingestQueueCapacity = v; }
    public long getEvictionIntervalMillis() { return evictionIntervalMillis; }
    public void setEvictionIntervalMillis(long v) { this.evictionIntervalMillis = v; }
}
