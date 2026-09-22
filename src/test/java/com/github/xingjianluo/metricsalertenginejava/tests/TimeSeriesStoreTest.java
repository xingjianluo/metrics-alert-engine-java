package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.SeriesAggregation;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 窗口边界、乱序 / 重复、标签隔离、时间戳越界与基数上限。 */
class TimeSeriesStoreTest {

    private static final Logger log = LoggerFactory.getLogger(TimeSeriesStoreTest.class);
    private static final long T0 = 1_000_000L;

    private ManualClock clock;
    private EngineProperties props;
    private TimeSeriesStore store;

    @BeforeEach
    void setUp() {
        clock = new ManualClock(T0 + 100_000);
        props = TestSupport.properties();
        store = new TimeSeriesStore(props, clock);
    }

    private WriteResult write(String metric, Labels labels, double v, long ts) {
        WriteResult r = store.ingest(new Sample(metric, labels, v, ts));
        log.info("[写入] metric={} labels={} value={} ts={} -> accepted={} reason={} detail={}",
                metric, labels, v, ts, r.accepted(), r.reason(), r.detail());
        return r;
    }

    @Test
    void windowBoundariesAreEpochAlignedAndHalfOpen() {
        Labels l = Labels.empty();
        // 窗口 [0,1000) 与 [1000,2000)，边界点 1000 必须落到后一个窗口
        write("m", l, 1, 999);
        write("m", l, 2, 0);
        write("m", l, 5, 1000);
        write("m", l, 7, 1999);

        List<SeriesAggregation> result = store.query("m", Map.of(), 0, 2000, 1000, AggregateFunction.SUM);
        assertEquals(1, result.size());
        List<SeriesAggregation.WindowValue> windows = result.get(0).windows();
        log.info("[查询] [0,2000) window=1000 SUM -> w0={} w1={}", windows.get(0).value(), windows.get(1).value());
        assertEquals(2, windows.size());
        assertEquals(0L, windows.get(0).windowStart());
        assertEquals(3.0, windows.get(0).value(), 1e-9, "0 和 999 属第一窗口");
        assertEquals(12.0, windows.get(1).value(), 1e-9, "边界点 1000 必须归入第二窗口");
    }

    @Test
    void missingWindowsAreMarkedNotZero() {
        write("m", Labels.empty(), 3, 5_000);
        List<SeriesAggregation> r = store.query("m", Map.of(), 0, 7_000, 1000, AggregateFunction.AVG);
        long missing = r.get(0).windows().stream().filter(SeriesAggregation.WindowValue::missing).count();
        log.info("[查询] 仅 5000 有数据，7 个窗口中缺失窗口数={}", missing);
        assertEquals(7, r.get(0).windows().size());
        assertEquals(6, missing, "空窗口必须标记 missing，不能与真实的 0 值混淆");
    }

    @Test
    void duplicateTimestampsAreRejectedAndNotDoubleCounted() {
        Labels l = Labels.of(Map.of("host", "a"));
        assertTrue(write("m", l, 1, 1000).accepted());
        WriteResult dup = write("m", l, 2, 1000);
        assertFalse(dup.accepted());
        assertEquals(RejectReason.DUPLICATE, dup.reason());
        // 乱序到达同样按时间戳去重
        WriteResult outOfOrderDup = write("m", l, 9, 1000);
        assertEquals(RejectReason.DUPLICATE, outOfOrderDup.reason());
        List<SeriesAggregation> r = store.query("m", Map.of(), 0, 2000, 1000, AggregateFunction.COUNT);
        log.info("[判定] 重复/乱序重复均拒绝；窗口采样数={}", r.get(0).windows().get(1).sampleCount());
        assertEquals(1, r.get(0).windows().get(1).sampleCount(), "同时间戳只能计一次");
    }

    @Test
    void labelCombinationsAreIsolated() {
        write("m", Labels.of(Map.of("host", "a")), 1, 1000);
        write("m", Labels.of(Map.of("host", "b")), 100, 1000);
        List<SeriesAggregation> all = store.query("m", Map.of(), 0, 2000, 1000, AggregateFunction.SUM);
        log.info("[查询] 无选择器命中时间线数={}，值={}", all.size(),
                all.stream().map(s -> s.labels() + "=" + s.windows().get(1).value()).toList());
        assertEquals(2, all.size(), "host=a 与 host=b 必须相互隔离");
        List<SeriesAggregation> onlyA = store.query("m", Map.of("host", "a"), 0, 2000, 1000, AggregateFunction.SUM);
        assertEquals(1, onlyA.size());
        assertEquals(1.0, onlyA.get(0).windows().get(1).value(), 1e-9);
    }

    @Test
    void rejectsTimestampOutOfRangeAndInvalidName() {
        WriteResult tooOld = write("m", Labels.empty(), 1, T0 - props.getRetentionMillis() - 1);
        assertEquals(RejectReason.TIMESTAMP_OUT_OF_RANGE, tooOld.reason());
        clock.set(T0 + 100_000);
        WriteResult tooFuture = write("m", Labels.empty(), 1,
                clock.nowMillis() + props.getFutureToleranceMillis() + 1);
        assertEquals(RejectReason.TIMESTAMP_OUT_OF_RANGE, tooFuture.reason());
        WriteResult badName = write("1bad-metric", Labels.empty(), 1, clock.nowMillis());
        assertEquals(RejectReason.INVALID_METRIC_NAME, badName.reason());
        log.info("[判定] 过旧={} 过未来={} 非法指标名={}，三类拒绝原因可区分",
                tooOld.reason(), tooFuture.reason(), badName.reason());
    }

    @Test
    void enforcesPerMetricAndGlobalSeriesCardinalityLimit() {
        props.setMaxSeriesPerMetric(3);
        props.setMaxTotalSeries(5);
        assertTrue(write("m", Labels.of(Map.of("h", "1")), 1, T0 + 1000).accepted());
        assertTrue(write("m", Labels.of(Map.of("h", "2")), 1, T0 + 1000).accepted());
        assertTrue(write("m", Labels.of(Map.of("h", "3")), 1, T0 + 1000).accepted());
        WriteResult fourth = write("m", Labels.of(Map.of("h", "4")), 1, T0 + 1000);
        assertEquals(RejectReason.CARDINALITY_LIMIT, fourth.reason());
        log.info("[判定] 第 4 条时间线被拒绝：{}，detail={}", fourth.reason(), fourth.detail());
        assertEquals(3, store.seriesCount("m"));
    }
}
