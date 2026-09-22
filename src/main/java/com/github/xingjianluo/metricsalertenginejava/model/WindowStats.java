package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.HashSet;
import java.util.Set;

/**
 * 单个窗口桶内的统计量（count/sum/min/max），自身负责并发安全。
 * 同一时间戳重复写入会被拒绝（按时间戳去重），保证乱序重复数据不双计。
 */
public final class WindowStats {

    private final long windowStartMillis;
    private final long windowSizeMillis;
    private final Set<Long> seenTimestamps = new HashSet<>();
    private long count;
    private double sum;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;

    public WindowStats(long windowStartMillis, long windowSizeMillis) {
        this.windowStartMillis = windowStartMillis;
        this.windowSizeMillis = windowSizeMillis;
    }

    private WindowStats(long windowStartMillis, long windowSizeMillis, long count, double sum,
                        double min, double max, Set<Long> seen) {
        this.windowStartMillis = windowStartMillis;
        this.windowSizeMillis = windowSizeMillis;
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.seenTimestamps.addAll(seen);
    }

    public long windowStartMillis() {
        return windowStartMillis;
    }

    public long windowSizeMillis() {
        return windowSizeMillis;
    }

    /** 合并一个采样值；返回 false 表示该时间戳重复，未计入。 */
    public synchronized boolean add(double value, long timestampMillis) {
        if (!seenTimestamps.add(timestampMillis)) {
            return false;
        }
        count++;
        sum += value;
        min = Math.min(min, value);
        max = Math.max(max, value);
        return true;
    }

    public synchronized long count() {
        return count;
    }

    public synchronized double sum() {
        return sum;
    }

    public synchronized double min() {
        return count == 0 ? Double.NaN : min;
    }

    public synchronized double max() {
        return count == 0 ? Double.NaN : max;
    }

    public synchronized double avg() {
        return count == 0 ? Double.NaN : sum / count;
    }

    /** 快照拷贝，供查询与告警评估使用。 */
    public synchronized WindowStats snapshot() {
        return new WindowStats(windowStartMillis, windowSizeMillis, count, sum,
                count == 0 ? Double.POSITIVE_INFINITY : min,
                count == 0 ? Double.NEGATIVE_INFINITY : max, seenTimestamps);
    }
}
