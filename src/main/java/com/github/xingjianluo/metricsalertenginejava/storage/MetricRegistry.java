package com.github.xingjianluo.metricsalertenginejava.storage;

import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 指标定义的版本化注册表。
 * 每个指标名保留全部历史版本（按版本号排序）：当前版本用于写入校验，
 * 历史版本用于按版本复现历史窗口与告警结论。并发安全。
 */
public final class MetricRegistry {

    private final Map<String, NavigableMap<Integer, MetricDefinition>> versions = new ConcurrentHashMap<>();
    private final int maxMetrics;

    public MetricRegistry(int maxMetrics) {
        this.maxMetrics = maxMetrics;
    }

    /** 注册新指标（v1）。时间范围由调用方依据保留策略给出。重名/超容量抛出 IllegalArgumentException。 */
    public synchronized MetricDefinition register(String name, java.util.Set<String> allowedLabelKeys,
                                                  int maxSeriesCardinality, long minTimestamp,
                                                  long maxTimestamp) {
        if (versions.containsKey(name)) {
            throw new IllegalArgumentException("metric.already.exists: metric " + name + " already registered");
        }
        if (versions.size() >= maxMetrics) {
            throw new IllegalArgumentException(
                    "metric.capacity.exceeded: max metrics " + maxMetrics + " reached, rejected " + name);
        }
        MetricDefinition def = new MetricDefinition(name, 1, allowedLabelKeys, maxSeriesCardinality,
                minTimestamp, maxTimestamp);
        NavigableMap<Integer, MetricDefinition> map = new ConcurrentSkipListMap<>();
        map.put(1, def);
        versions.put(name, map);
        return def;
    }

    /** 更新指标定义：允许标签键或基数变化（也允许无变化的再发布），分配下一版本号。 */
    public MetricDefinition update(String name, java.util.Set<String> allowedLabelKeys, int maxSeriesCardinality) {
        NavigableMap<Integer, MetricDefinition> map = versions.get(name);
        if (map == null) {
            throw new IllegalArgumentException("metric.unknown: cannot update unknown metric " + name);
        }
        synchronized (map) {
            MetricDefinition latest = map.lastEntry().getValue();
            int newVersion = latest.version() + 1;
            MetricDefinition next = latest.withVersion(newVersion, allowedLabelKeys, maxSeriesCardinality);
            map.put(newVersion, next);
            return next;
        }
    }

    public MetricDefinition current(String name) {
        NavigableMap<Integer, MetricDefinition> map = versions.get(name);
        return map == null ? null : map.lastEntry().getValue();
    }

    /** 按版本号取定义；version 为 null 时取当前版本；不存在返回 null。 */
    public MetricDefinition version(String name, Integer version) {
        NavigableMap<Integer, MetricDefinition> map = versions.get(name);
        if (map == null) {
            return null;
        }
        return version == null ? map.lastEntry().getValue() : map.get(version);
    }

    /** 全部历史版本，按版本号升序。 */
    public List<MetricDefinition> history(String name) {
        NavigableMap<Integer, MetricDefinition> map = versions.get(name);
        return map == null ? List.of() : new ArrayList<>(map.values());
    }
}
