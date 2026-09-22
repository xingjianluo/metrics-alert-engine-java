package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.rule.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.rule.EvaluationRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.LOG;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.MINUTE;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.T0;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.labels;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.logEvaluation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 版本化测试：
 * - 指标定义升级（新增允许标签键）后，旧版本定义仍拒绝新标签键的数据，写入按当前版本校验；
 * - 规则升级（阈值 10 -> 100）后，对同一历史窗口分别按 v1/v2 复现，结论可不同但各自确定；
 * - 规则状态机按 (规则版本, 标签) 隔离，v1 的历史状态不受 v2 评估影响。
 */
class VersioningTest {

    @Test
    void metricDefinitionVersionsAreReproducible() {
        var holder = TestSupport.defaultEngine();
        holder.at(T0 + MINUTE);
        var engine = holder.engine();
        var v1 = engine.registerMetric("cpu", Set.of("host"), 100, 60 * MINUTE);
        assertEquals(1, v1.version());

        // v1 下只允许 host
        WriteResult r1 = engine.write(new Sample("cpu", labels("pod", "x"), 1, T0));
        LOG.info("[metric-version] v1 拒绝 pod 标签: {}", r1.message());
        assertFalse(r1.accepted());
        assertEquals(RejectReason.INVALID_LABEL, r1.reason());

        // 升级到 v2：新增 pod 标签键
        var v2 = engine.updateMetric("cpu", Set.of("host", "pod"), 100);
        assertEquals(2, v2.version());
        LOG.info("[metric-version] 指标升级: {} -> {}", v1, v2);

        // 当前版本允许 pod
        WriteResult r2 = engine.write(new Sample("cpu", labels("pod", "x"), 1, T0 + 10_000));
        LOG.info("[metric-version] v2 接受 pod 标签: {}", r2);
        assertTrue(r2.accepted());
        assertEquals(2, r2.metricVersion());

        // 历史定义仍可取回，允许标签键集合保持 v1 原貌（可复现）
        var fetchedV1 = engine.metricRegistry().version("cpu", 1);
        assertEquals(Set.of("host"), fetchedV1.allowedLabelKeys());
        assertEquals(2, engine.metricRegistry().history("cpu").size());

        // 按 v1 标注查询历史窗口，聚合版本标注为 1（数据点本身不随定义变化）
        var aggV1 = engine.queryWindow("cpu", labels("pod", "x"), T0, MINUTE, 1,
                List.of(Aggregator.COUNT));
        var aggV2 = engine.queryWindow("cpu", labels("pod", "x"), T0, MINUTE, 2,
                List.of(Aggregator.COUNT));
        LOG.info("[metric-version] 同窗口按版本查询 metricVersion 标注: v1={}, v2={}",
                aggV1.metricVersion(), aggV2.metricVersion());
        assertEquals(1, aggV1.metricVersion());
        assertEquals(2, aggV2.metricVersion());
        assertEquals(1L, aggV2.count());
    }

    @Test
    void ruleVersionChangeProducesReproducibleHistoricalConclusions() {
        var holder = TestSupport.defaultEngine();
        holder.at(T0 + 10 * MINUTE);
        var engine = holder.engine();
        engine.registerMetric("cpu", Set.of("host"), 100, 60 * MINUTE);
        var labels = labels("host", "a");
        // v1：SUM > 10 触发（pending=1）
        engine.registerRule("cpu-high", "cpu", Aggregator.SUM, AlertRule.Operator.GT, 10.0,
                1, 1, MINUTE, 0, Set.of());
        for (int w = 0; w < 3; w++) {
            engine.write(new Sample("cpu", labels, 20, T0 + w * MINUTE));
        }

        // 按 v1 评估三个窗口：w0 即触发，w1/w2 持续命中保持 FIRING
        var v1Results = engine.evaluateRange("cpu-high", labels, T0, T0 + 3 * MINUTE, 1, null);
        for (EvaluationRecord r : v1Results) {
            logEvaluation("rule-v1", r);
        }
        assertEquals(AlertState.FIRING, engine.alertState("cpu-high", 1, labels));
        assertTrue(v1Results.get(0).hit());
        assertEquals(AlertState.FIRING, v1Results.get(0).stateAfter());

        // 规则升级 v2：阈值 100（同样的历史数据 20 不再命中）
        engine.updateRule("cpu-high", AlertRule.Operator.GT, Aggregator.SUM, 100.0,
                1, 1, 0, Set.of());
        assertEquals(2, engine.ruleRegistry().current("cpu-high").version());
        var v2Results = engine.evaluateRange("cpu-high", labels, T0, T0 + 3 * MINUTE, 2, null);
        for (EvaluationRecord r : v2Results) {
            logEvaluation("rule-v2", r);
        }
        // v2 下始终未命中，状态保持 RESOLVED
        assertEquals(AlertState.RESOLVED, engine.alertState("cpu-high", 2, labels));
        assertFalse(v2Results.get(0).hit());

        // v1 的历史结论原封不动（状态、记录均独立）
        assertEquals(AlertState.FIRING, engine.alertState("cpu-high", 1, labels));
        var v1History = engine.evaluationHistory("cpu-high", 1, labels);
        var v2History = engine.evaluationHistory("cpu-high", 2, labels);
        assertEquals(3, v1History.size());
        assertEquals(3, v2History.size());
        LOG.info("[rule-version] v1 历史={} 条, v2 历史={} 条，互不影响", v1History.size(), v2History.size());

        // 可重复复现：再取一次 v1 历史记录，输入值与判定依据完全一致
        assertEquals(v1History.get(0).inputValue(), v2History.get(0).inputValue());
        assertEquals(true, v1History.get(0).hit());
        assertEquals(false, v2History.get(0).hit());

        // 不能倒序重放任一版本
        assertThrows(IllegalStateException.class,
                () -> engine.evaluateWindow("cpu-high", labels, T0, 1, null));
    }
}
