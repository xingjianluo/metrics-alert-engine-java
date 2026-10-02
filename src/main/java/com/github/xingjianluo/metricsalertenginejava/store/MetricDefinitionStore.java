package com.github.xingjianluo.metricsalertenginejava.store;

import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 指标定义版本库：同一 metric 的每次更新产生单调递增的新版本，旧版本不可变并永久保留，
 * 因此针对任意历史定义版本都可以取回到当时的口径（单位、默认聚合、范围、标签键白名单）。
 *
 * <p>并发更新在 per-metric 的锁内分配版本号，保证不会出现两个版本同号；
 * 容量策略（指标数、版本数上限）由 {@code MetricDefinitionService} 在调用前判定，
 * 本类只负责版本存储，不做计数。
 */
public class MetricDefinitionStore {

    /** metric -> 按版本号有序的全部定义版本 */
    private final ConcurrentHashMap<String, NavigableMap<Integer, MetricDefinition>> definitions =
            new ConcurrentHashMap<>();

    /** 登记首个版本（v1）；已存在则抛 {@link IllegalStateException}。 */
    public MetricDefinition register(String metric, MetricDefinition.Spec spec, long createdAtMillis) {
        NavigableMap<Integer, MetricDefinition> versions =
                definitions.computeIfAbsent(metric, ignored -> new ConcurrentSkipListMap<>());
        synchronized (versions) {
            if (!versions.isEmpty()) {
                throw new IllegalStateException("metric definition already exists: " + metric
                        + "; use update() to create a new version");
            }
            MetricDefinition definition = toDefinition(new MetricVersion(metric, 1), spec, createdAtMillis);
            versions.put(1, definition);
            return definition;
        }
    }

    /** 产生下一版本；从未登记则抛 {@link IllegalArgumentException}。 */
    public MetricDefinition update(String metric, MetricDefinition.Spec spec, long createdAtMillis) {
        NavigableMap<Integer, MetricDefinition> versions = definitions.get(metric);
        if (versions == null) {
            throw new IllegalArgumentException("metric definition not found: " + metric);
        }
        synchronized (versions) {
            int nextVersion = versions.lastKey() + 1;
            MetricDefinition definition =
                    toDefinition(new MetricVersion(metric, nextVersion), spec, createdAtMillis);
            versions.put(nextVersion, definition);
            return definition;
        }
    }

    private static MetricDefinition toDefinition(MetricVersion version, MetricDefinition.Spec spec,
                                                 long createdAtMillis) {
        return new MetricDefinition(version, spec.unit(), spec.description(), spec.defaultAggregation(),
                spec.minValue(), spec.maxValue(), spec.allowedLabelKeys(), createdAtMillis);
    }

    /** 最新版本；未登记返回 null（由服务层决定宽松/严格语义）。 */
    public MetricDefinition latest(String metric) {
        NavigableMap<Integer, MetricDefinition> versions = definitions.get(metric);
        return versions == null || versions.isEmpty() ? null : versions.lastEntry().getValue();
    }

    /** 取回指定版本；不存在抛 {@link IllegalArgumentException}。 */
    public MetricDefinition get(MetricVersion version) {
        NavigableMap<Integer, MetricDefinition> versions = definitions.get(version.metric());
        MetricDefinition definition = versions == null ? null : versions.get(version.version());
        if (definition == null) {
            throw new IllegalArgumentException("metric definition version not found: " + version);
        }
        return definition;
    }

    /** 该指标的全部定义版本（按版本号升序）；未登记返回空列表。 */
    public List<MetricDefinition> versions(String metric) {
        NavigableMap<Integer, MetricDefinition> versions = definitions.get(metric);
        return versions == null ? List.of() : new ArrayList<>(versions.values());
    }

    /** 已登记定义的指标数量。 */
    public int metricCount() {
        return definitions.size();
    }

    /** 某指标当前已保留的定义版本数；未登记为 0。 */
    public int versionCount(String metric) {
        NavigableMap<Integer, MetricDefinition> versions = definitions.get(metric);
        return versions == null ? 0 : versions.size();
    }
}
