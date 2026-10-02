package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.List;

/**
 * 带指标定义版本信息的聚合查询结果：让调用方看得出结论是用哪个版本的口径解读的。
 */
public record MetricQueryResult(
        String metric,
        MetricVersion definitionVersion,
        boolean definitionRegistered,
        AggregateFunction function,
        List<SeriesAggregation> series) {
}
