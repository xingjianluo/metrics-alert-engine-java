package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricQueryResult;
import com.github.xingjianluo.metricsalertenginejava.model.SeriesAggregation;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 查询服务：委托存储在时间线点快照上做滚动窗口聚合。
 * 每次查询独立获取快照，因此并发写入下读到的始终是某个时刻自洽的数据。
 * <p>指标定义解读：查询可指定一个历史定义版本（不指定则用最新版本），
 * 该版本的默认聚合口径、允许标签键、取值范围共同决定如何解读已落库的数据；
 * 同一次查询只钉住一个版本，结论自洽且可按版本复现。
 * 未登记定义的指标保持宽松模式：不做任何过滤，版本标记为 null。
 */
public class QueryService {

    private final TimeSeriesStore store;
    private final MetricDefinitionStore definitionStore;

    /** 兼容构造：不带指标定义库（全部指标按未登记处理）。 */
    public QueryService(TimeSeriesStore store) {
        this.store = store;
        this.definitionStore = null;
    }

    public QueryService(TimeSeriesStore store, MetricDefinitionStore definitionStore) {
        this.store = store;
        this.definitionStore = definitionStore;
    }

    /** 宽松查询：不做定义解读，行为与引入指标定义前完全一致。 */
    public List<SeriesAggregation> query(String metric,
                                         Map<String, String> selectors,
                                         long fromMillis,
                                         long toMillis,
                                         long windowMillis,
                                         AggregateFunction function) {
        return store.query(metric, selectors, fromMillis, toMillis, windowMillis, function);
    }

    /**
     * 按指标定义解读的查询。
     *
     * @param function         显式聚合算子；为 null 时取所钉住定义版本的默认口径
     * @param definitionVersion 指定解读用的定义版本；为 null 时用最新版本
     * @return 结果附带实际使用的定义版本（未登记指标为 null）与聚合算子
     */
    public MetricQueryResult queryWithDefinition(String metric,
                                                 Map<String, String> selectors,
                                                 long fromMillis,
                                                 long toMillis,
                                                 long windowMillis,
                                                 AggregateFunction function,
                                                 Integer definitionVersion) {
        // 一次查询只钉住一个版本：版本对象不可变，因此整次查询解读自洽。
        Optional<MetricDefinition> definition = resolveDefinition(metric, definitionVersion);
        if (definition.isEmpty()) {
            // 未登记定义：宽松模式，不过滤；算子缺省 SUM（与历史默认一致）。
            AggregateFunction fn = function != null ? function : AggregateFunction.SUM;
            return new MetricQueryResult(metric, null, fn,
                    store.query(metric, selectors, fromMillis, toMillis, windowMillis, fn));
        }
        MetricDefinition def = definition.get();
        AggregateFunction fn = function != null ? function : def.defaultFunction();
        List<SeriesAggregation> series = store.query(metric, selectors, fromMillis, toMillis,
                windowMillis, fn,
                labels -> labels.asMap().keySet().stream().allMatch(def::labelKeyAllowed),
                def::valueInRange);
        return new MetricQueryResult(metric, def.version(), fn, series);
    }

    private Optional<MetricDefinition> resolveDefinition(String metric, Integer version) {
        if (definitionStore == null) {
            return Optional.empty();
        }
        if (version == null) {
            return definitionStore.latest(metric);
        }
        // 指定了版本但指标未登记 / 版本不存在：明确报错，而不是悄悄退回宽松模式。
        return Optional.of(definitionStore.get(metric, version));
    }
}
