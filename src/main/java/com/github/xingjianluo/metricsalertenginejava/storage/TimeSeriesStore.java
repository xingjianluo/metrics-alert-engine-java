package com.github.xingjianluo.metricsalertenginejava.storage;

import com.github.xingjianluo.metricsalertenginejava.model.DropStats;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 全部指标的内存时序存储。
 * 以读写锁保护“序列映射 + 单序列结构”：查询在共享锁下取快照，写入在排他锁下进行，
 * 保证并发写入与查询看到的快照自洽（同一时刻的序列集合与序列内容一致）。
 */
public final class TimeSeriesStore {

    private final java.util.Map<String, java.util.Map<String, Series>> seriesByMetric = new java.util.HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final int maxPointsPerSeries;
    private final DropStats dropStats;

    public TimeSeriesStore(int maxPointsPerSeries, DropStats dropStats) {
        this.maxPointsPerSeries = maxPointsPerSeries;
        this.dropStats = dropStats;
    }

    /**
     * 写入样本。def 为该指标当前版本定义（调用方完成注册表查找）。
     * 校验顺序：值 -> 标签 -> 时间戳 -> 基数，产生可区分拒绝原因。
     */
    public WriteResult write(Sample sample, com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition def) {
        // TODO 骨架
        return null;
    }

    /** 单窗口 [windowStart, windowStart+size) 聚合输入点的快照。 */
    public List<Series.Point> snapshotWindow(String metric, LabelSet labels, long windowStart, long size) {
        // TODO 骨架
        return List.of();
    }

    /** 区间 [from, to) 快照。 */
    public List<Series.Point> snapshotRange(String metric, LabelSet labels, long from, long to) {
        // TODO 骨架
        return List.of();
    }

    public int seriesCount(String metric) {
        // TODO 骨架
        return 0;
    }

    public int totalSeriesCount() {
        // TODO 骨架
        return 0;
    }

    public DropStats dropStats() {
        return dropStats;
    }

    ReentrantReadWriteLock lock() {
        return lock;
    }
}
