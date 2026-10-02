package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricQueryResult;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;
import com.github.xingjianluo.metricsalertenginejava.model.SeriesAggregation;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;

import java.util.List;
import java.util.Map;

/**
 * 查询服务：委托存储在时间线点快照上做滚动窗口聚合。
 * 每次查询独立获取快照，因此并发写入下读到的始终是某个时刻自洽的数据。
 *
 * <p>指标定义版本语义：
 * <ul>
 *   <li>指定 {@code definitionVersion}：用该历史版本的默认聚合口径解读同一批已落库数据，
 *       结论与该版本当时一致（数据不变，口径回放到旧版本）；</li>
 *   <li>不指定版本：走当前最新定义版本的默认聚合口径；</li>
 *   <li>指标从未登记定义：必须显式提供 {@code function}，保持宽松模式，版本标记为 null；</li>
 *   <li>返回的 {@link MetricQueryResult} 明确标出本次使用的定义版本与实际算子。</li>
 * </ul>
 */
public class QueryService {

    private final TimeSeriesStore store;
    private final MetricDefinitionStore definitionStore;

    /** 不启用指标定义解读（未登记一律按调用方显式算子聚合）。 */
    public QueryService(TimeSeriesStore store) {
        this(store, null);
    }

    public QueryService(TimeSeriesStore store, MetricDefinitionStore definitionStore) {
        this.store = store;
        this.definitionStore = definitionStore;
    }

    /** 无版本、无定义的直接聚合（兼容旧调用）。 */
    public List<SeriesAggregation> query(String metric,
                                         Map<String, String> selectors,
                                         long fromMillis,
                                         long toMillis,
                                         long windowMillis,
                                         AggregateFunction function) {
        return store.query(metric, selectors, fromMillis, toMillis, windowMillis, function);
    }

    /**
     * 按指标定义版本解读的聚合查询。
     *
     * @param definitionVersion 指定历史定义版本；null 表示最新版本
     * @param function          显式算子；非 null 时优先于定义默认口径；
     *                          为 null 时使用所解析定义版本的默认聚合
     */
    public MetricQueryResult queryWithDefinition(String metric,
                                                 Map<String, String> selectors,
                                                 long fromMillis,
                                                 long toMillis,
                                                 long windowMillis,
                                                 AggregateFunction function,
                                                 MetricVersion definitionVersion) {
        MetricDefinition definition = resolveDefinition(metric, definitionVersion);
        AggregateFunction effectiveFunction = function;
        if (effectiveFunction == null) {
            if (definition == null) {
                throw new IllegalArgumentException(
                        "function is required when metric has no registered definition: " + metric);
            }
            effectiveFunction = definition.defaultAggregation();
        }
        List<SeriesAggregation> series =
                store.query(metric, selectors, fromMillis, toMillis, windowMillis, effectiveFunction);
        MetricVersion resolvedVersion = definition == null ? null : definition.version();
        return new MetricQueryResult(metric, resolvedVersion, definition != null,
                effectiveFunction, series);
    }

    private MetricDefinition resolveDefinition(String metric, MetricVersion requested) {
        if (definitionStore == null) {
            return null;
        }
        if (requested != null) {
            if (!requested.metric().equals(metric)) {
                throw new IllegalArgumentException("definition version " + requested
                        + " does not belong to metric " + metric);
            }
            return definitionStore.get(requested);
        }
        return definitionStore.latest(metric);
    }
}
