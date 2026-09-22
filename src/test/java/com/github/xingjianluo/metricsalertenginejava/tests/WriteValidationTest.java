package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.engine.EngineConfig;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.engine.MetricsAlertEngine;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.LOG;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.MINUTE;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.T0;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.labels;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.logInput;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.logResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 写入路径校验测试：各类非法数据必须被拒绝且拒绝原因可区分，绝不静默丢弃。
 */
class WriteValidationTest {

    private MetricsAlertEngine newEngine() {
        // 极小基数与保留期，便于触达边界；时钟停在 T0，合法范围 [T0-10min, T0+60s]
        EngineConfig config = new EngineConfig(4, 2, 100, 4, 8,
                10 * MINUTE, MINUTE);
        return TestSupport.engine(config).engine();
    }

    @Test
    void acceptsValidWriteAndRejectsEachBadCaseWithDistinctReason() {
        MetricsAlertEngine engine = newEngine();
        engine.registerMetric("cpu", Set.of("host", "dc"), 2, 10 * MINUTE);

        // 1) 正常写入
        WriteResult ok = engine.write(new Sample("cpu", labels("host", "a", "dc", "x"), 0.9, T0));
        logInput("write-valid", "cpu{host=a,dc=x}=0.9 @T0");
        logResult("write-valid", ok);
        assertTrue(ok.accepted());

        // 2) 未知指标
        WriteResult unknown = engine.write(new Sample("mem", LabelSet.empty(), 1.0, T0));
        logInput("write-unknown", "mem=1.0 @T0");
        logResult("write-unknown", unknown);
        assertFalse(unknown.accepted());
        assertEquals(RejectReason.METRIC_UNKNOWN, unknown.reason());

        // 3) 空指标名
        WriteResult blankName = engine.write(new Sample("  ", LabelSet.empty(), 1.0, T0));
        logResult("write-blank-name", blankName);
        assertEquals(RejectReason.INVALID_METRIC_NAME, blankName.reason());

        // 4) 非法标签：值含分隔符
        WriteResult badLabel = engine.write(new Sample("cpu", labels("host", "a|b"), 0.5, T0));
        logInput("write-bad-label", "cpu{host=a|b}=0.5");
        logResult("write-bad-label", badLabel);
        assertFalse(badLabel.accepted());
        assertEquals(RejectReason.INVALID_LABEL, badLabel.reason());
        assertTrue(badLabel.message().contains("label.value.separator"), badLabel.message());

        // 5) 非法标签：使用未声明的标签键
        WriteResult badKey = engine.write(new Sample("cpu", labels("pod", "z"), 0.5, T0));
        logInput("write-unknown-label-key", "cpu{pod=z}=0.5");
        logResult("write-unknown-label-key", badKey);
        assertEquals(RejectReason.INVALID_LABEL, badKey.reason());
        assertTrue(badKey.message().contains("label.key.not.allowed"), badKey.message());

        // 6) 时间戳越界（早于下界）
        WriteResult tooOld = engine.write(new Sample("cpu", labels("host", "a"), 0.5, T0 - 11 * MINUTE));
        logInput("write-too-old", "ts=T0-11min");
        logResult("write-too-old", tooOld);
        assertEquals(RejectReason.TIMESTAMP_OUT_OF_RANGE, tooOld.reason());

        // 7) 时间戳越界（晚于上界 T0+60s）
        WriteResult tooNew = engine.write(new Sample("cpu", labels("host", "a"), 0.5, T0 + 2 * MINUTE));
        logResult("write-too-new", tooNew);
        assertEquals(RejectReason.TIMESTAMP_OUT_OF_RANGE, tooNew.reason());

        // 8) 非法值（NaN）
        WriteResult nan = engine.write(new Sample("cpu", labels("host", "a"), Double.NaN, T0));
        logResult("write-nan", nan);
        assertEquals(RejectReason.INVALID_VALUE, nan.reason());

        // 9) 基数上限：已存在 host=a/dc=x 一条，再来 host=b 是第二条（=2，允许），host=c 为第三条应拒绝
        WriteResult second = engine.write(new Sample("cpu", labels("host", "b"), 0.1, T0));
        logResult("write-second-series", second);
        assertTrue(second.accepted());
        WriteResult third = engine.write(new Sample("cpu", labels("host", "c"), 0.1, T0));
        logInput("write-cardinality", "existing=2 limit=2 new host=c");
        logResult("write-cardinality", third);
        assertFalse(third.accepted());
        assertEquals(RejectReason.CARDINALITY_EXCEEDED, third.reason());

        // 守恒断言：最终只有 2 条序列
        assertEquals(2, engine.seriesCount("cpu"));
        LOG.info("[write-cardinality] seriesCount(cpu)={} (守恒)", engine.seriesCount("cpu"));

        // 10) 被拒绝的样本没有产生任何数据点：查询其窗口聚合 count=0
        long w = T0 - (T0 % MINUTE);
        var agg = engine.queryWindow("cpu", labels("host", "c"), w, MINUTE, null,
                java.util.List.of(com.github.xingjianluo.metricsalertenginejava.model.Aggregator.COUNT));
        assertEquals(0L, agg.count());
        logResult("write-cardinality", "rejected series window count=" + agg.count());
    }

    @Test
    void duplicateTimestampIsIdempotentOverwriteNotDoubleCounted() {
        MetricsAlertEngine engine = newEngine();
        engine.registerMetric("cpu", Set.of("host"), 2, 10 * MINUTE);
        WriteResult first = engine.write(new Sample("cpu", labels("host", "a"), 1.0, T0));
        WriteResult second = engine.write(new Sample("cpu", labels("host", "a"), 5.0, T0));
        logInput("dup-ts", "same timestamp written twice: 1.0 then 5.0");
        logResult("dup-ts-first", first);
        logResult("dup-ts-second", second);
        assertTrue(first.accepted());
        assertFalse(first.duplicate());
        assertTrue(second.accepted());
        assertTrue(second.duplicate());

        long w = T0 - (T0 % MINUTE);
        var agg = engine.queryWindow("cpu", labels("host", "a"), w, MINUTE, null,
                java.util.List.of(com.github.xingjianluo.metricsalertenginejava.model.Aggregator.values()));
        // 只有一个点，值以后写为准
        assertEquals(1L, agg.count());
        assertEquals(5.0, agg.get(com.github.xingjianluo.metricsalertenginejava.model.Aggregator.SUM));
        logResult("dup-ts", agg);
    }
}
