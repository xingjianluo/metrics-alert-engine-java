package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.engine.MetricsAlertEngine;
import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WindowAggregate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.LOG;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.MINUTE;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.T0;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.labels;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 窗口语义测试：
 * - epoch 固定对齐，边界点 [start, start+size) 归属确定（含左不含右）；
 * - 乱序写入后聚合结果与顺序写入完全一致；
 * - 同一指标不同标签组合相互隔离；
 * - 空窗口 SUM/COUNT=0，AVG/MIN/MAX=null。
 */
class WindowBoundaryTest {

    private final MetricsAlertEngine engine;

    {
        var holder = TestSupport.defaultEngine();
        holder.at(T0 + 10 * MINUTE); // 覆盖窗口数据到 T0+10min
        engine = holder.engine();
    }
    private final LabelSet hostA = labels("host", "a");
    private final LabelSet hostB = labels("host", "b");

    @Test
    void tumblingWindowBoundariesAreHalfOpenAndEpochAligned() {
        engine.registerMetric("cpu", Set.of("host"), 100, 60 * MINUTE);
        long w0 = T0; // T0 已对齐整分钟
        // 左边界点归入 w0
        engine.write(new Sample("cpu", hostA, 10, w0));
        // 窗口内点归入 w0
        engine.write(new Sample("cpu", hostA, 20, w0 + 30_000));
        // 右边界点归入 w1
        engine.write(new Sample("cpu", hostA, 30, w0 + MINUTE));

        LOG.info("[window-boundary] INPUT w0={} pts: {}@w0, {}@w0+30s, {}@w0+60s(w1)",
                w0, 10, 20, 30);

        WindowAggregate a0 = engine.queryWindow("cpu", hostA, w0, MINUTE, null,
                List.of(Aggregator.SUM, Aggregator.COUNT, Aggregator.AVG, Aggregator.MIN, Aggregator.MAX));
        WindowAggregate a1 = engine.queryWindow("cpu", hostA, w0 + MINUTE, MINUTE, null,
                List.of(Aggregator.SUM, Aggregator.COUNT));
        LOG.info("[window-boundary] RESULT w0={} w1={}", a0, a1);

        assertEquals(2L, a0.count());
        assertEquals(30.0, a0.get(Aggregator.SUM));
        assertEquals(15.0, a0.get(Aggregator.AVG));
        assertEquals(10.0, a0.get(Aggregator.MIN));
        assertEquals(20.0, a0.get(Aggregator.MAX));
        assertEquals(1L, a1.count());
        assertEquals(30.0, a1.get(Aggregator.SUM));
    }

    @Test
    void outOfOrderWritesProduceSameAggregateAsOrdered() {
        engine.registerMetric("reqs", Set.of("host"), 100, 60 * MINUTE);
        // 故意乱序：先写窗口中部，再补更早点与右边界附近点；TreeMap 保证聚合与写入顺序无关
        long[] offsets = {50_000, 1_000, 59_000, 10_000};
        double[] vals = {40, 10, 60, 20};
        for (int i = 0; i < offsets.length; i++) {
            engine.write(new Sample("reqs", hostA, vals[i], T0 + offsets[i]));
            LOG.info("[unordered] INPUT reqs{{host=a}}={} @relative {}ms (乱序写入)", vals[i], offsets[i]);
        }
        // 只请求 SUM/COUNT/AVG：未请求的函数键不存在
        WindowAggregate partial = engine.queryWindow("reqs", hostA, T0, MINUTE, null,
                List.of(Aggregator.SUM, Aggregator.COUNT, Aggregator.AVG));
        LOG.info("[unordered] RESULT partial aggregate={}", partial);
        assertEquals(4L, partial.count());
        assertEquals(130.0, partial.get(Aggregator.SUM));
        assertEquals(32.5, partial.get(Aggregator.AVG));
        assertNull(partial.get(Aggregator.MIN));
        assertNull(partial.get(Aggregator.MAX));

        // 全函数再查一次，核对乱序数据的 MIN/MAX 仍正确
        WindowAggregate all = engine.queryWindow("reqs", hostA, T0, MINUTE, null,
                List.of(Aggregator.values()));
        LOG.info("[unordered] RESULT full aggregate={}", all);
        assertEquals(10.0, all.get(Aggregator.MIN));
        assertEquals(60.0, all.get(Aggregator.MAX));

        // 确定性：再查一次结果必须逐字段相等（可重复）
        WindowAggregate again = engine.queryWindow("reqs", hostA, T0, MINUTE, null,
                List.of(Aggregator.values()));
        assertEquals(all.functions(), again.functions());
        assertEquals(all.count(), again.count());
    }

    @Test
    void labelCombinationsAreIsolated() {
        engine.registerMetric("cpu", Set.of("host", "dc"), 100, 60 * MINUTE);
        var ax = labels("host", "a", "dc", "x");
        var bx = labels("host", "b", "dc", "x");
        engine.write(new Sample("cpu", ax, 1, T0));
        engine.write(new Sample("cpu", ax, 3, T0 + 1000));
        engine.write(new Sample("cpu", bx, 9, T0 + 1000));
        LOG.info("[isolation] INPUT ax=1,3 bx=9 @same window");

        WindowAggregate gAx = engine.queryWindow("cpu", ax, T0, MINUTE, null, List.of(Aggregator.SUM));
        WindowAggregate gBx = engine.queryWindow("cpu", bx, T0, MINUTE, null, List.of(Aggregator.SUM));
        LOG.info("[isolation] RESULT ax={} bx={}", gAx, gBx);
        assertEquals(4.0, gAx.get(Aggregator.SUM));
        assertEquals(9.0, gBx.get(Aggregator.SUM));
    }

    @Test
    void emptyWindowHasZeroSumCountAndNullAvgMinMax() {
        engine.registerMetric("cpu", Set.of("host"), 100, 60 * MINUTE);
        WindowAggregate empty = engine.queryWindow("cpu", hostA, T0, MINUTE, null,
                List.of(Aggregator.values()));
        LOG.info("[empty-window] RESULT {}", empty);
        assertEquals(0L, empty.count());
        assertEquals(0.0, empty.get(Aggregator.SUM));
        assertEquals(0.0, empty.get(Aggregator.COUNT));
        assertNull(empty.get(Aggregator.AVG));
        assertNull(empty.get(Aggregator.MIN));
        assertNull(empty.get(Aggregator.MAX));
        assertTrue(empty.functions().containsKey(Aggregator.AVG));
    }
}
