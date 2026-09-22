package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.SeriesAggregation;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;

import java.util.List;
import java.util.Map;

/**
 * 查询服务：委托存储在时间线点快照上做滚动窗口聚合。
 * 每次查询独立获取快照，因此并发写入下读到的始终是某个时刻自洽的数据。
 */
public class QueryService {

    private final TimeSeriesStore store;

    public QueryService(TimeSeriesStore store) {
        this.store = store;
    }

    public List<SeriesAggregation> query(String metric,
                                         Map<String, String> selectors,
                                         long fromMillis,
                                         long toMillis,
                                         long windowMillis,
                                         AggregateFunction function) {
        return store.query(metric, selectors, fromMillis, toMillis, windowMillis, function);
    }
}
