package com.github.xingjianluo.metricsalertenginejava.storage;

import com.github.xingjianluo.metricsalertenginejava.model.DropStats;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 全部指标的内存时序存储。
 * 以读写锁保护“序列映射 + 单序列结构”：查询在共享锁下取不可变点快照，写入在排他锁下进行，
 * 保证并发写入与查询看到的快照自洽（同一时刻看到的序列集合与各序列内容一致）。
 */
public final class TimeSeriesStore {

    private final Map<String, Map<String, Series>> seriesByMetric = new java.util.HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final int maxPointsPerSeries;
    private final DropStats dropStats;

    public TimeSeriesStore(int maxPointsPerSeries, DropStats dropStats) {
        this.maxPointsPerSeries = maxPointsPerSeries;
        this.dropStats = dropStats;
    }

    /**
     * 写入样本。def 为该指标当前版本定义（调用方完成注册表查找与 METRIC_UNKNOWN 判定）。
     * 校验顺序固定，保证拒绝原因可区分：
     * INVALID_VALUE -> INVALID_LABEL -> TIMESTAMP_OUT_OF_RANGE -> CARDINALITY_EXCEEDED。
     */
    public WriteResult write(Sample sample, MetricDefinition def) {
        LabelSet labels = sample.labels();

        if (!Double.isFinite(sample.value())) {
            return WriteResult.rejected(RejectReason.INVALID_VALUE,
                    "value not finite: " + sample.value(), def.version());
        }
        try {
            labels.validate();
        } catch (IllegalArgumentException e) {
            return WriteResult.rejected(RejectReason.INVALID_LABEL, e.getMessage(), def.version());
        }
        for (String key : labels.labels().keySet()) {
            if (!def.allowedLabelKeys().contains(key)) {
                return WriteResult.rejected(RejectReason.INVALID_LABEL,
                        "label.key.not.allowed: key '" + key + "' is not in allowed set "
                                + def.allowedLabelKeys() + " for metric " + def.name(),
                        def.version());
            }
        }
        if (sample.timestampMillis() < def.minTimestampMillis()
                || sample.timestampMillis() > def.maxTimestampMillis()) {
            return WriteResult.rejected(RejectReason.TIMESTAMP_OUT_OF_RANGE,
                    "timestamp " + sample.timestampMillis() + " out of range ["
                            + def.minTimestampMillis() + ", " + def.maxTimestampMillis() + "] for metric "
                            + def.name(), def.version());
        }

        lock.writeLock().lock();
        try {
            Map<String, Series> metricSeries =
                    seriesByMetric.computeIfAbsent(sample.metric(), k -> new java.util.HashMap<>());
            String key = labels.seriesKey();
            Series series = metricSeries.get(key);
            if (series == null) {
                if (metricSeries.size() >= def.maxSeriesCardinality()) {
                    return WriteResult.rejected(RejectReason.CARDINALITY_EXCEEDED,
                            "series cardinality " + metricSeries.size() + " reached limit "
                                    + def.maxSeriesCardinality() + " for metric " + def.name()
                                    + "; rejected labels=" + labels, def.version());
                }
                series = new Series(sample.metric(), labels, maxPointsPerSeries, dropStats);
                metricSeries.put(key, series);
            }
            boolean duplicate = series.put(sample.timestampMillis(), sample.value());
            return duplicate ? WriteResult.okDuplicate(def.version()) : WriteResult.ok(def.version());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 单窗口 [windowStart, windowStart+size) 内点的不可变快照；序列不存在时为空列表。 */
    public List<Series.Point> snapshotWindow(String metric, LabelSet labels, long windowStart, long size) {
        return snapshotRange(metric, labels, windowStart, windowStart + size);
    }

    /** 区间 [from, to) 点快照，按时间升序；序列不存在时为空列表。 */
    public List<Series.Point> snapshotRange(String metric, LabelSet labels, long from, long to) {
        lock.readLock().lock();
        try {
            Map<String, Series> metricSeries = seriesByMetric.get(metric);
            if (metricSeries == null) {
                return List.of();
            }
            Series series = metricSeries.get(labels.seriesKey());
            if (series == null) {
                return List.of();
            }
            return series.range(from, to);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int seriesCount(String metric) {
        lock.readLock().lock();
        try {
            Map<String, Series> metricSeries = seriesByMetric.get(metric);
            return metricSeries == null ? 0 : metricSeries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public int totalSeriesCount() {
        lock.readLock().lock();
        try {
            int total = 0;
            for (Map<String, Series> m : seriesByMetric.values()) {
                total += m.size();
            }
            return total;
        } finally {
            lock.readLock().unlock();
        }
    }

    public DropStats dropStats() {
        return dropStats;
    }

    ReentrantReadWriteLock lock() {
        return lock;
    }
}
