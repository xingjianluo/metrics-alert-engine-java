package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;

/** 周期淘汰过期数据，防止内存无限增长。 */
public class RetentionService {

    private final TimeSeriesStore store;
    private final EngineClock clock;

    public RetentionService(TimeSeriesStore store, EngineClock clock) {
        this.store = store;
        this.clock = clock;
    }

    public int evictOnce() {
        return store.evict(clock.nowMillis());
    }
}
