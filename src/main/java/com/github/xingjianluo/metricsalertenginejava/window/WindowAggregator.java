package com.github.xingjianluo.metricsalertenginejava.window;

import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.WindowAggregate;
import com.github.xingjianluo.metricsalertenginejava.storage.Series;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * 固定对齐的滚动窗口工具：windowStart = floor(ts / windowSize) * windowSize（epoch 对齐）。
 * 同一批样本点在给定窗口内做确定性聚合；空窗口的 SUM/COUNT 为 0，AVG/MIN/MAX 为 null。
 */
public final class WindowAggregator {

    private WindowAggregator() {
    }

    public static long windowStart(long timestampMillis, long windowSizeMillis) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("window.size.invalid: " + windowSizeMillis);
        }
        return Math.floorDiv(timestampMillis, windowSizeMillis) * windowSizeMillis;
    }

    /** 对已经按窗口截取好的点集做聚合。 */
    public static WindowAggregate aggregate(String metric, LabelSet labels, long windowStart, long windowSize,
                                            int metricVersion, List<Series.Point> points,
                                            Iterable<Aggregator> requiredFunctions) {
        Map<Aggregator, Double> all = allFunctions(points);
        Map<Aggregator, Double> selected = new EnumMap<>(Aggregator.class);
        for (Aggregator function : requiredFunctions) {
            selected.put(function, all.get(function));
        }
        return new WindowAggregate(metric, labels, windowStart, windowSize, metricVersion,
                points.size(), Collections.unmodifiableMap(new EnumMap<>(selected)));
    }

    /** 计算全部五种聚合函数。空点集：SUM=0, COUNT=0，其余为 null。 */
    public static Map<Aggregator, Double> allFunctions(List<Series.Point> points) {
        Map<Aggregator, Double> result = new EnumMap<>(Aggregator.class);
        long count = points.size();
        result.put(Aggregator.COUNT, (double) count);
        if (count == 0) {
            result.put(Aggregator.SUM, 0.0);
            result.put(Aggregator.AVG, null);
            result.put(Aggregator.MIN, null);
            result.put(Aggregator.MAX, null);
            return Collections.unmodifiableMap(new EnumMap<>(result));
        }
        double sum = 0.0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (Series.Point p : points) {
            double v = p.value();
            sum += v;
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        result.put(Aggregator.SUM, sum);
        result.put(Aggregator.AVG, sum / count);
        result.put(Aggregator.MIN, min);
        result.put(Aggregator.MAX, max);
        return Collections.unmodifiableMap(new EnumMap<>(result));
    }
}
