package com.github.xingjianluo.metricsalertenginejava.window;

import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.WindowAggregate;
import com.github.xingjianluo.metricsalertenginejava.storage.Series;

import java.util.List;
import java.util.Map;

/**
 * 固定对齐的滚动窗口工具：windowStart = floor(ts / windowSize) * windowSize。
 * 同一批样本点在给定窗口内做确定性聚合；空窗口的 SUM/COUNT 为 0，AVG/MIN/MAX 为 null。
 */
public final class WindowAggregator {

    private WindowAggregator() {
    }

    public static long windowStart(long timestampMillis, long windowSizeMillis) {
        // TODO 骨架
        return 0L;
    }

    /** 对已经按窗口截取好的点集做聚合。 */
    public static WindowAggregate aggregate(String metric, LabelSet labels, long windowStart, long windowSize,
                                            int metricVersion, List<Series.Point> points,
                                            Iterable<Aggregator> requiredFunctions) {
        // TODO 骨架
        return null;
    }

    /** 计算全部五种聚合函数。 */
    public static Map<Aggregator, Double> allFunctions(List<Series.Point> points) {
        // TODO 骨架
        return Map.of();
    }
}
