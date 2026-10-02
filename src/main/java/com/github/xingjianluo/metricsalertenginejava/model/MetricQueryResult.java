package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.List;

/**
 * 一次窗口聚合查询的结果，附带解读所用的指标定义版本，
 * 使“同一批数据在不同版本口径下的结论”可区分、可复现。
 *
 * @param metric            指标名
 * @param definitionVersion 解读所用的指标定义版本；指标未登记定义时为 null（宽松模式）
 * @param function          实际使用的聚合算子（显式指定优先，否则取该版本默认口径）
 * @param series            各时间线的窗口聚合结果
 */
public record MetricQueryResult(
        String metric,
        Integer definitionVersion,
        AggregateFunction function,
        List<SeriesAggregation> series) {
}
