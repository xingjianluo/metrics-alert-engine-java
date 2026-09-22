package com.github.xingjianluo.metricsalertenginejava.storage;

import com.github.xingjianluo.metricsalertenginejava.model.DropReason;
import com.github.xingjianluo.metricsalertenginejava.model.DropStats;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;

import java.util.List;
import java.util.TreeMap;

/**
 * 单条时间序列：同一指标 + 同一 LabelSet。
 * 样本按时间戳保存在 TreeMap 中，天然支持乱序插入与重复时间戳幂等覆盖；
 * 每条序列有容量上限，超限按“丢弃最旧样本”降级并记录 RETENTION_TRIM。
 */
public final class Series {

    private final String metric;
    private final LabelSet labels;
    private final TreeMap<Long, Double> points = new TreeMap<>();
    private final int maxPoints;
    private final DropStats dropStats;

    public Series(String metric, LabelSet labels, int maxPoints, DropStats dropStats) {
        this.metric = metric;
        this.labels = labels;
        this.maxPoints = maxPoints;
        this.dropStats = dropStats;
    }

    public String metric() {
        return metric;
    }

    public LabelSet labels() {
        return labels;
    }

    /** 写入样本；返回 true 表示该时间戳此前已有值（重复覆盖）。调用方负责加锁。 */
    boolean put(long timestampMillis, double value) {
        // TODO 骨架
        return false;
    }

    /** 取时间区间 [from, to) 的快照。调用方负责加锁。 */
    List<Point> range(long fromMillis, long toMillis) {
        // TODO 骨架
        return List.of();
    }

    int size() {
        // TODO 骨架
        return 0;
    }

    /** 不可变样本点。 */
    public record Point(long timestampMillis, double value) {
    }
}
