package com.github.xingjianluo.metricsalertenginejava.storage;

import com.github.xingjianluo.metricsalertenginejava.model.DropReason;
import com.github.xingjianluo.metricsalertenginejava.model.DropStats;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * 单条时间序列：同一指标 + 同一 LabelSet。
 * 样本按时间戳保存在 TreeMap 中，天然支持乱序插入与重复时间戳幂等覆盖；
 * 每条序列有容量上限，超限按“丢弃最旧样本”降级并记录 RETENTION_TRIM，
 * 保证写入路径不被慢消费/长滞留拖垮。
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

    /**
     * 写入样本；返回 true 表示该时间戳此前已有值（重复，按最后写入覆盖）。
     * 超过容量时淘汰最旧点并记录 RETENTION_TRIM（一次写入最多淘汰一个点）。
     * 调用方负责加锁。
     */
    boolean put(long timestampMillis, double value) {
        boolean duplicate = points.containsKey(timestampMillis);
        points.put(timestampMillis, value);
        if (points.size() > maxPoints) {
            Long oldest = points.firstKey();
            points.remove(oldest);
            dropStats.record(DropReason.RETENTION_TRIM,
                    "metric=" + metric + " labels=" + labels + " trimmedTs=" + oldest
                            + " retained=" + maxPoints);
        }
        return duplicate;
    }

    /** 取时间区间 [from, to) 的不可变点快照，按时间升序。调用方负责加锁。 */
    List<Point> range(long fromMillis, long toMillis) {
        var sub = points.subMap(fromMillis, true, toMillis, false);
        List<Point> out = new ArrayList<>(sub.size());
        for (var e : sub.entrySet()) {
            out.add(new Point(e.getKey(), e.getValue()));
        }
        return List.copyOf(out);
    }

    int size() {
        return points.size();
    }

    /** 不可变样本点。 */
    public record Point(long timestampMillis, double value) {
    }
}
