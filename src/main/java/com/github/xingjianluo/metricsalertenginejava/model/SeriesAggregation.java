package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.List;

/**
 * 一次聚合查询中单个时间线（metric+labels）的窗口序列结果。
 */
public record SeriesAggregation(
        String metric,
        Labels labels,
        List<WindowValue> windows) {

    /**
     * 单个窗口的聚合值；missing=true 表示该窗口没有数据（不补零，保证判定可解释）。
     */
    public record WindowValue(long windowStart, long windowEnd, double value, boolean missing, long sampleCount) {
    }
}
