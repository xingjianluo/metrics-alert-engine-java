package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricQueryResult;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;
import com.github.xingjianluo.metricsalertenginejava.model.SeriesAggregation;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 查询服务：委托存储在时间线点快照上做滚动窗口聚合。
 * 每次查询独立获取快照，因此并发写入下读到的始终是某个时刻自洽的数据。
 *
 * <p>指标定义版本语义：
 * <ul>
 *   <li>指定 {@code definitionVersion}：用该历史版本的<b>完整口径</b>解读同一批已落库数据——
 *       默认聚合、取值范围、标签键白名单三者同时生效：该版本不会接受的点（越界值）
 *       与不会接受的时间线（含白名单外标签键）都不进入结果，
 *       结论与该版本当时一致（数据不变，口径回放到旧版本）；</li>
 *   <li>不指定版本：走当前最新定义版本的完整口径；</li>
 *   <li>指标从未登记定义：必须显式提供 {@code function}，保持宽松模式（不过滤点与时间线），
 *       版本标记为 null；</li>
 *   <li>单次查询只解析一次定义版本，整个结果（算子 + 过滤口径 + 版本标记）整体自洽，
 *       并发定义更新不会让一次查询一半按旧口径一半按新口径；</li>
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
        List<SeriesAggregation> series = definition == null
                // 未登记定义：宽松路径，不过滤任何点与时间线（与接入定义能力前一致）。
                ? store.query(metric, selectors, fromMillis, toMillis, windowMillis, effectiveFunction)
                // 已登记定义：按解析到的这一版完整口径（值域 + 标签键白名单）过滤后再聚合。
                : queryUnderDefinition(metric, selectors, fromMillis, toMillis, windowMillis,
                        effectiveFunction, definition);
        MetricVersion resolvedVersion = definition == null ? null : definition.version();
        return new MetricQueryResult(metric, resolvedVersion, definition != null,
                effectiveFunction, series);
    }

    /**
     * 按某一版定义的完整口径解读已落库数据：
     * <ul>
     *   <li>标签键白名单启用时，含有白名单外标签键的<b>整条时间线</b>被排除
     *       （该版本当时不会接受这条时间线的写入）；</li>
     *   <li>声明了取值范围时，范围外的<b>单个点</b>被剔除
     *       （该版本当时不会接受这个点，含 NaN/无穷大）；</li>
     *   <li>剩余点在快照上聚合，与并发写入/定义更新互不影响。</li>
     * </ul>
     */
    private List<SeriesAggregation> queryUnderDefinition(String metric,
                                                         Map<String, String> selectors,
                                                         long fromMillis,
                                                         long toMillis,
                                                         long windowMillis,
                                                         AggregateFunction function,
                                                         MetricDefinition definition) {
        List<SeriesAggregation> results = new ArrayList<>();
        for (TimeSeriesStore.SeriesSnapshot snapshot : store.snapshotMatching(metric, selectors)) {
            // 时间线级过滤：该版本白名单之外的标签键 -> 整条线不计入。
            if (!definition.disallowedKeys(snapshot.labels()).isEmpty()) {
                continue;
            }
            // 点级过滤：该版本值域之外的点 -> 不计入（未声明范围时不过滤）。
            NavigableMap<Long, Double> points = snapshot.points();
            if (definition.hasValueRange()) {
                NavigableMap<Long, Double> inRange = new TreeMap<>();
                points.forEach((t, v) -> {
                    if (definition.isValueInRange(v)) {
                        inRange.put(t, v);
                    }
                });
                points = inRange;
            }
            List<SeriesAggregation.WindowValue> windows = store.aggregateWindows(points,
                    fromMillis, toMillis, windowMillis, function);
            results.add(new SeriesAggregation(metric, snapshot.labels(), windows));
        }
        results.sort(Comparator.comparing(s -> s.labels().canonicalString()));
        return results;
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
