package com.github.xingjianluo.metricsalertenginejava.store;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.EngineClock;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;

/**
 * 指标定义版本库：同一指标的每次更新产生单调递增的新版本，旧版本不可变并永久保留，
 * 因此任意历史版本口径下的查询结论都可复现。
 * <p>容量保护：定义总数（{@code max-metric-definitions}）与单指标版本数
 * （{@code max-definition-versions-per-metric}）均有上限；超限按明确策略拒绝
 * （抛 {@link DefinitionLimitException}）并计入 {@link MetricsRegistry} 的
 * {@code definitionLimitRejected} 计数，绝不悄悄吞掉。
 * <p>并发语义：版本对象不可变，发布后通过并发Map对读写线程可见；
 * 登记 / 更新在 per-metric 版本表上串行化，版本号不会跳号或重复。
 */
public class MetricDefinitionStore {

    private static final Pattern METRIC_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");
    private static final Pattern LABEL_KEY = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    /** 不含指标名与版本号的定义内容，用于登记 / 更新。 */
    public record DefinitionSpec(String unit,
                                 String description,
                                 AggregateFunction defaultFunction,
                                 Double minValue,
                                 Double maxValue,
                                 Set<String> allowedLabelKeys) {
    }

    /** 定义数量 / 版本数量超限时的明确拒绝信号（调用方据此走降级策略）。 */
    public static class DefinitionLimitException extends RuntimeException {
        public DefinitionLimitException(String message) {
            super(message);
        }
    }

    private final EngineProperties properties;
    private final EngineClock clock;
    private final MetricsRegistry registry;

    /** metric -> 按版本号有序的全部定义版本 */
    private final ConcurrentHashMap<String, NavigableMap<Integer, MetricDefinition>> definitions =
            new ConcurrentHashMap<>();

    public MetricDefinitionStore(EngineProperties properties, EngineClock clock,
                                 MetricsRegistry registry) {
        this.properties = properties;
        this.clock = clock;
        this.registry = registry;
    }

    /** 登记某指标的首个定义版本（v1）；已存在时抛 IllegalStateException。 */
    public MetricDefinition register(String metric, DefinitionSpec spec) {
        validate(metric, spec);
        // 登记是低频控制面操作：整体串行化，容量检查与插入原子完成。
        synchronized (definitions) {
            NavigableMap<Integer, MetricDefinition> versions = definitions.get(metric);
            if (versions != null) {
                throw new IllegalStateException("definition already exists for metric: " + metric
                        + "; use update() to create a new version");
            }
            // 容量保护：定义总数超限 -> 明确拒绝 + 可观测计数（不悄悄吞掉）。
            if (definitions.size() >= properties.getMaxMetricDefinitions()) {
                registry.recordDefinitionLimitRejected();
                throw new DefinitionLimitException("metric definition limit "
                        + properties.getMaxMetricDefinitions() + " exceeded; rejected metric=" + metric);
            }
            versions = new ConcurrentSkipListMap<>();
            MetricDefinition definition = toDefinition(metric, 1, spec);
            versions.put(1, definition);
            definitions.put(metric, versions);
            return definition;
        }
    }

    /** 更新某指标的定义，产生单调递增的新版本；旧版本不可变保留。 */
    public MetricDefinition update(String metric, DefinitionSpec spec) {
        validate(metric, spec);
        NavigableMap<Integer, MetricDefinition> versions = definitions.get(metric);
        if (versions == null) {
            throw new IllegalArgumentException("definition not found for metric: " + metric);
        }
        synchronized (versions) {
            // 容量保护：单指标版本数超限 -> 明确拒绝 + 可观测计数；旧版本不受影响。
            if (versions.size() >= properties.getMaxDefinitionVersionsPerMetric()) {
                registry.recordDefinitionLimitRejected();
                throw new DefinitionLimitException("definition version limit "
                        + properties.getMaxDefinitionVersionsPerMetric()
                        + " exceeded for metric=" + metric);
            }
            int nextVersion = versions.lastKey() + 1;
            MetricDefinition definition = toDefinition(metric, nextVersion, spec);
            versions.put(nextVersion, definition);
            return definition;
        }
    }

    /** 最新版本定义；未登记时为空（调用方按宽松模式处理，兼容未登记写入）。 */
    public Optional<MetricDefinition> latest(String metric) {
        NavigableMap<Integer, MetricDefinition> versions = definitions.get(metric);
        if (versions == null || versions.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(versions.lastEntry().getValue());
    }

    /** 取指定历史版本；不存在时抛 IllegalArgumentException。 */
    public MetricDefinition get(String metric, int version) {
        NavigableMap<Integer, MetricDefinition> versions = definitions.get(metric);
        MetricDefinition definition = versions == null ? null : versions.get(version);
        if (definition == null) {
            throw new IllegalArgumentException("definition version not found: " + metric + "#v" + version);
        }
        return definition;
    }

    /** 某指标的全部版本（按版本号升序）；未登记时为空表。 */
    public List<MetricDefinition> versions(String metric) {
        NavigableMap<Integer, MetricDefinition> versions = definitions.get(metric);
        return versions == null ? List.of() : new ArrayList<>(versions.values());
    }

    /** 当前已登记定义的指标数量。 */
    public int definitionCount() {
        return definitions.size();
    }

    private MetricDefinition toDefinition(String metric, int version, DefinitionSpec spec) {
        return new MetricDefinition(metric, version, spec.unit(), spec.description(),
                spec.defaultFunction(), spec.minValue(), spec.maxValue(),
                spec.allowedLabelKeys() == null ? Set.of() : Set.copyOf(spec.allowedLabelKeys()),
                clock.nowMillis());
    }

    private static void validate(String metric, DefinitionSpec spec) {
        if (metric == null || !METRIC_NAME.matcher(metric).matches()) {
            throw new IllegalArgumentException("invalid metric name: " + metric);
        }
        if (spec == null) {
            throw new IllegalArgumentException("definition spec is required");
        }
        if (spec.defaultFunction() == null) {
            throw new IllegalArgumentException("defaultFunction is required");
        }
        if (spec.minValue() != null && spec.maxValue() != null
                && spec.minValue() > spec.maxValue()) {
            throw new IllegalArgumentException("minValue must be <= maxValue: ["
                    + spec.minValue() + "," + spec.maxValue() + "]");
        }
        if (spec.allowedLabelKeys() != null) {
            for (String key : spec.allowedLabelKeys()) {
                if (key == null || !LABEL_KEY.matcher(key).matches()) {
                    throw new IllegalArgumentException("invalid allowed label key: " + key);
                }
            }
        }
    }
}
