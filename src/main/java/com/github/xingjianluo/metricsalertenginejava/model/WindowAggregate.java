package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.Map;

/**
 * 一个滚动窗口 [windowStartMillis, windowStartMillis+windowSizeMillis) 内
 * 某条序列的聚合结果。functions 为每种聚合函数的结果；count 为参与聚合的样本数。
 */
public record WindowAggregate(String metric, LabelSet labels, long windowStartMillis, long windowSizeMillis,
                              int metricVersion, long count, Map<Aggregator, Double> functions) {

    public Double get(Aggregator aggregator) {
        return functions.get(aggregator);
    }

    @Override
    public String toString() {
        return "WindowAggregate{" + metric + labels + " w=" + windowStartMillis
                + " count=" + count + " " + functions + '}';
    }
}
