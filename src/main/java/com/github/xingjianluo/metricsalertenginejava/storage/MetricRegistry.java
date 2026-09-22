package com.github.xingjianluo.metricsalertenginejava.storage;

import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 指标定义的版本化注册表。
 * 每个指标名保留全部历史版本（按版本号排序），当前版本可用于写入校验，
 * 历史版本可用于按版本复现历史窗口与告警结论。
 */
public final class MetricRegistry {

    private final Map<String, NavigableMap<Integer, MetricDefinition>> versions = new ConcurrentHashMap<>();
    private final int maxMetrics;

    public MetricRegistry(int maxMetrics) {
        this.maxMetrics = maxMetrics;
    }

    /** 注册新指标（v1）。重名/非法名/超容量时抛出 IllegalArgumentException。 */
    public MetricDefinition register(String name, java.util.Set<String> allowedLabelKeys,
                                     int maxSeriesCardinality, long minTimestamp, long maxTimestamp) {
        // TODO 骨架
        return null;
    }

    /** 更新指标定义：允许标签键或基数变化时创建新版本，返回新版本定义。 */
    public MetricDefinition update(String name, java.util.Set<String> allowedLabelKeys, int maxSeriesCardinality) {
        // TODO 骨架
        return null;
    }

    public MetricDefinition current(String name) {
        // TODO 骨架
        return null;
    }

    /** 按版本号取定义；version 为 null 时取当前版本。 */
    public MetricDefinition version(String name, Integer version) {
        // TODO 骨架
        return null;
    }

    public List<MetricDefinition> history(String name) {
        // TODO 骨架
        return List.of();
    }
}
