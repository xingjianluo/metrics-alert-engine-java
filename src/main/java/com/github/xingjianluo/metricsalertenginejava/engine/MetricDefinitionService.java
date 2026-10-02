package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;

import java.util.List;
import java.util.regex.Pattern;

/**
 * 指标定义登记 / 更新 / 按版本取回服务。
 *
 * <ul>
 *   <li>首次登记产生 v1，之后每次更新产生新版本，旧版本由存储永久保留；</li>
 *   <li>容量上限：指标定义总数 {@code maxMetricDefinitions}、单指标版本数
 *       {@code maxDefinitionVersionsPerMetric}，超限以
 *       {@link MetricDefinitionLimitException} 显式拒绝（原因
 *       {@link RejectReason#DEFINITION_LIMIT_EXCEEDED}）并累加
 *       {@code definitionsRejected} 计数，绝不静默吞掉；</li>
 *   <li>容量判定与版本号分配串行进行，并发更新不会超配也不会产生同号版本。</li>
 * </ul>
 */
public class MetricDefinitionService {

    private static final Pattern METRIC_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    private final MetricDefinitionStore store;
    private final MetricsRegistry registry;
    private final EngineProperties properties;
    private final EngineClock clock;
    /** 定义管理是低频操作：用一把管理锁把“容量判定—登记/更新”串成原子步骤。 */
    private final Object adminLock = new Object();

    public MetricDefinitionService(MetricDefinitionStore store, MetricsRegistry registry,
                                   EngineProperties properties, EngineClock clock) {
        this.store = store;
        this.registry = registry;
        this.properties = properties;
        this.clock = clock;
    }

    /** 首次登记定义（v1）；指标名非法抛 IllegalArgumentException，容量超限抛 MetricDefinitionLimitException。 */
    public MetricDefinition register(String metric, MetricDefinition.Spec spec) {
        validateMetricName(metric);
        requireSpec(spec);
        synchronized (adminLock) {
            if (store.metricCount() >= properties.getMaxMetricDefinitions()) {
                return rejectByLimit("metric definition count limit "
                        + properties.getMaxMetricDefinitions() + " exceeded for new metric " + metric);
            }
            MetricDefinition definition = store.register(metric, spec, clock.nowMillis());
            registry.recordDefinitionCreated();
            return definition;
        }
    }

    /** 更新定义并产生新版本；未登记抛 IllegalArgumentException，版本数超限抛 MetricDefinitionLimitException。 */
    public MetricDefinition update(String metric, MetricDefinition.Spec spec) {
        validateMetricName(metric);
        requireSpec(spec);
        synchronized (adminLock) {
            if (store.versionCount(metric) == 0) {
                throw new IllegalArgumentException("metric definition not found: " + metric);
            }
            if (store.versionCount(metric) >= properties.getMaxDefinitionVersionsPerMetric()) {
                return rejectByLimit("definition version limit "
                        + properties.getMaxDefinitionVersionsPerMetric()
                        + " exceeded for metric " + metric + "; existing versions are retained");
            }
            MetricDefinition definition = store.update(metric, spec, clock.nowMillis());
            registry.recordDefinitionUpdated();
            return definition;
        }
    }

    /** 存在则更新产生新版本，不存在则登记为 v1（与规则 PUT 的 upsert 语义一致）。 */
    public MetricDefinition registerOrUpdate(String metric, MetricDefinition.Spec spec) {
        validateMetricName(metric);
        requireSpec(spec);
        synchronized (adminLock) {
            long now = clock.nowMillis();
            if (store.versionCount(metric) == 0) {
                if (store.metricCount() >= properties.getMaxMetricDefinitions()) {
                    return rejectByLimit("metric definition count limit "
                            + properties.getMaxMetricDefinitions() + " exceeded for new metric " + metric);
                }
                MetricDefinition definition = store.register(metric, spec, now);
                registry.recordDefinitionCreated();
                return definition;
            }
            if (store.versionCount(metric) >= properties.getMaxDefinitionVersionsPerMetric()) {
                return rejectByLimit("definition version limit "
                        + properties.getMaxDefinitionVersionsPerMetric()
                        + " exceeded for metric " + metric + "; existing versions are retained");
            }
            MetricDefinition definition = store.update(metric, spec, now);
            registry.recordDefinitionUpdated();
            return definition;
        }
    }

    private MetricDefinition rejectByLimit(String message) {
        registry.recordDefinitionRejected();
        throw new MetricDefinitionLimitException(RejectReason.DEFINITION_LIMIT_EXCEEDED, message);
    }

    private static void requireSpec(MetricDefinition.Spec spec) {
        if (spec == null) {
            throw new IllegalArgumentException("definition spec is required");
        }
        // 触发 Spec 紧凑构造器中的语义校验。
        new MetricDefinition.Spec(spec.unit(), spec.description(), spec.defaultAggregation(),
                spec.minValue(), spec.maxValue(), spec.allowedLabelKeys());
    }

    private static void validateMetricName(String metric) {
        if (metric == null || !METRIC_NAME.matcher(metric).matches()) {
            throw new IllegalArgumentException("invalid metric name: " + metric);
        }
    }

    /** 最新定义；未登记返回 null，调用方据此走未登记的宽松语义。 */
    public MetricDefinition latest(String metric) {
        return store.latest(metric);
    }

    public MetricDefinition get(MetricVersion version) {
        return store.get(version);
    }

    public List<MetricDefinition> versions(String metric) {
        return store.versions(metric);
    }

    public int metricCount() {
        return store.metricCount();
    }
}
