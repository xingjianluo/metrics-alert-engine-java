package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.engine.MetricsAlertEngine;
import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.rule.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.rule.EvaluationRecord;
import com.github.xingjianluo.metricsalertenginejava.rule.NotificationRecord;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 告警状态机语义测试：
 * PENDING（连续命中待定）-> FIRING（触发）-> 连续未命中 -> RESOLVED（恢复）；
 * 抖动（命中/未命中交替）不得重复触发、不得错误恢复；
 * 静默期只抑制通知发送，不抑制状态迁移与记录；窗口必须按序求值。
 */
class AlertStateMachineTest {

    private TestSupport.EngineWithClock newEngineWithRule(long silenceMillis) {
        var holder = TestSupport.defaultEngine();
        // 把时钟推进到覆盖全部测试窗口（T0+20min），注册后合法范围约 [T0-24h, T0+21min]
        holder.at(T0 + 20 * MINUTE);
        var engine = holder.engine();
        engine.registerMetric("cpu", Set.of("host"), 100, 24 * 60 * MINUTE);
        // 1 分钟窗口、SUM>10 命中；连续 3 次命中触发，连续 2 次未命中恢复
        engine.registerRule("cpu-high", "cpu", Aggregator.SUM, AlertRule.Operator.GT, 10.0,
                3, 2, MINUTE, silenceMillis, Set.of());
        return holder;
    }

    private void writeChecked(MetricsAlertEngine engine,
                              com.github.xingjianluo.metricsalertenginejava.model.LabelSet labels,
                              double value, long ts) {
        WriteResult r = engine.write(new Sample("cpu", labels, value, ts));
        assertTrue(r.accepted(), () -> "sample should be accepted: " + r.message());
    }

    private void evalAndLog(MetricsAlertEngine engine,
                            com.github.xingjianluo.metricsalertenginejava.model.LabelSet labels,
                            long window, String scenario) {
        EvaluationRecord r = engine.evaluateWindow("cpu-high", labels, window, null, null);
        logEvaluation(scenario, r);
        var n = engine.notificationHistory("cpu-high", null, labels);
        if (!n.isEmpty() && n.get(n.size() - 1).windowStart() == window) {
            LOG.info("[{}] NOTIFY *** {}", scenario, n.get(n.size() - 1).message());
        }
    }

    @Test
    void pendingThenFireThenResolveWithDeterministicTransitions() {
        var engine = newEngineWithRule(0).engine();
        var labels = labels("host", "a");
        String scenario = "lifecycle";
        LOG.info("[{}] INPUT rule SUM>10, pending=3, resolve=2, 1-min window", scenario);

        // w0..w2 连续 3 个窗口命中
        for (int w = 0; w <= 2; w++) {
            writeChecked(engine, labels, 11, T0 + w * MINUTE);
            evalAndLog(engine, labels, T0 + w * MINUTE, scenario);
        }
        assertEquals(AlertState.FIRING, engine.alertState("cpu-high", null, labels));
        List<NotificationRecord> fires = engine.notificationHistory("cpu-high", 1, labels);
        assertEquals(1, fires.size());
        assertEquals(AlertState.FIRING, fires.get(0).notifyState());
        assertFalse(fires.get(0).suppressed());

        // w3 继续命中：保持 FIRING，不得重复触发
        writeChecked(engine, labels, 100, T0 + 3 * MINUTE);
        evalAndLog(engine, labels, T0 + 3 * MINUTE, scenario);
        assertEquals(AlertState.FIRING, engine.alertState("cpu-high", null, labels));
        assertEquals(1, engine.notificationHistory("cpu-high", 1, labels).size());

        // w4 第一次未命中：仍 FIRING（需要连续 2 次）
        writeChecked(engine, labels, 0, T0 + 4 * MINUTE);
        evalAndLog(engine, labels, T0 + 4 * MINUTE, scenario);
        assertEquals(AlertState.FIRING, engine.alertState("cpu-high", null, labels));

        // w5 第二次连续未命中：恢复
        writeChecked(engine, labels, 0, T0 + 5 * MINUTE);
        evalAndLog(engine, labels, T0 + 5 * MINUTE, scenario);
        assertEquals(AlertState.RESOLVED, engine.alertState("cpu-high", null, labels));
        List<NotificationRecord> all = engine.notificationHistory("cpu-high", 1, labels);
        assertEquals(2, all.size());
        assertEquals(AlertState.RESOLVED, all.get(1).notifyState());

        // 守恒：每次状态迁移恰好对应一条通知记录
        long transitions = engine.evaluationHistory("cpu-high", 1, labels).stream()
                .filter(r -> r.transitionReason() != null).count();
        LOG.info("[{}] transitions={} notifications={}", scenario, transitions, all.size());
        assertEquals(transitions, all.size());
    }

    @Test
    void flappingDuringPendingResetsHitsAndNeverFiresEarly() {
        var engine = newEngineWithRule(0).engine();
        var labels = labels("host", "a");
        String scenario = "flap-pending";
        // 命中、命中、未命中、命中、命中、命中 -> 触发只能发生在最后一个窗口
        boolean[] hits = {true, true, false, true, true, true};
        for (int w = 0; w < hits.length; w++) {
            writeChecked(engine, labels, hits[w] ? 11 : 0, T0 + w * MINUTE);
            evalAndLog(engine, labels, T0 + w * MINUTE, scenario);
        }
        assertEquals(AlertState.FIRING, engine.alertState("cpu-high", null, labels));
        var notes = engine.notificationHistory("cpu-high", 1, labels);
        assertEquals(1, notes.size());
        assertEquals(T0 + 5 * MINUTE, notes.get(0).windowStart());
        LOG.info("[{}] 抖动中触发窗口={} (期望 T0+5min)，无提前触发", scenario, notes.get(0).windowStart());
    }

    @Test
    void flappingDuringFiringResetsMissesAndDoesNotResolveOrRefire() {
        var engine = newEngineWithRule(0).engine();
        var labels = labels("host", "a");
        String scenario = "flap-firing";
        // 先触发
        for (int w = 0; w < 3; w++) {
            writeChecked(engine, labels, 11, T0 + w * MINUTE);
            engine.evaluateWindow("cpu-high", labels, T0 + w * MINUTE, null, null);
        }
        // FIRING 中抖动：miss, hit, miss, hit —— 任意两次 miss 都被 hit 打断，
        // 既不能达到连续 2 次 miss 而恢复，也不能重复触发
        boolean[] hits = {false, true, false, true};
        for (int i = 0; i < hits.length; i++) {
            long w = T0 + (3 + i) * MINUTE;
            writeChecked(engine, labels, hits[i] ? 11 : 0, w);
            evalAndLog(engine, labels, w, scenario);
        }
        assertEquals(AlertState.FIRING, engine.alertState("cpu-high", null, labels));
        var notes = engine.notificationHistory("cpu-high", 1, labels);
        assertEquals(1, notes.size(), "抖动期间不得产生任何新通知（不重复触发、不错误恢复）");
        LOG.info("[{}] 抖动后仍只有初始 1 条触发通知，状态={}", scenario, AlertState.FIRING);
    }

    @Test
    void silenceSuppressesNotificationButStateStillTransitionsAndIsRecorded() {
        // 静默期 10 分钟：测试时钟固定，w0..w2 内的触发通知应被抑制
        var engine = newEngineWithRule(10 * MINUTE).engine();
        var labels = labels("host", "a");
        String scenario = "silence";
        for (int w = 0; w < 3; w++) {
            writeChecked(engine, labels, 11, T0 + w * MINUTE);
            evalAndLog(engine, labels, T0 + w * MINUTE, scenario);
        }
        assertEquals(AlertState.FIRING, engine.alertState("cpu-high", null, labels),
                "静默期不得阻止状态迁移到 FIRING");
        var notes = engine.notificationHistory("cpu-high", 1, labels);
        assertEquals(1, notes.size(), "被抑制的迁移仍有通知记录");
        assertTrue(notes.get(0).suppressed());
        assertNotNull(notes.get(0).message());
        LOG.info("[{}] 静默期内触发通知被抑制但已记录: {}", scenario, notes.get(0).message());
    }

    @Test
    void outOfOrderEvaluationIsRejectedAndSameWindowCannotBeReplayed() {
        var engine = newEngineWithRule(0).engine();
        var labels = labels("host", "a");
        writeChecked(engine, labels, 11, T0);
        writeChecked(engine, labels, 11, T0 + MINUTE);
        engine.evaluateWindow("cpu-high", labels, T0, null, null);
        engine.evaluateWindow("cpu-high", labels, T0 + MINUTE, null, null);

        // 跳过中间窗口（已评 w0,w1，直接评 w3，缺 w2）：必须拒绝，空窗口同样要驱动计数，不能略过
        IllegalStateException exGap = assertThrows(IllegalStateException.class,
                () -> engine.evaluateWindow("cpu-high", labels, T0 + 3 * MINUTE, null, null));
        LOG.info("[ordered] 跳跃窗口被拒绝: {}", exGap.getMessage());
        assertTrue(exGap.getMessage().contains("window.gap"));

        // 回退/重复到旧窗口同样拒绝（同一条错误码族 out.of.order）
        IllegalStateException exReplay = assertThrows(IllegalStateException.class,
                () -> engine.evaluateWindow("cpu-high", labels, T0, null, null));
        LOG.info("[ordered] 乱序/重复求值被拒绝: {}", exReplay.getMessage());
        assertTrue(exReplay.getMessage().contains("out.of.order"));

        // 按顺序补齐评估：空窗口 w2（无数据，SUM=0 未命中）使 PENDING 命中计数清零但仍停留 PENDING，
        // w3 命中重新计数 hit=1（仍为 PENDING）
        EvaluationRecord emptyW2 = engine.evaluateWindow("cpu-high", labels, T0 + 2 * MINUTE, null, null);
        logEvaluation("ordered-gap-fill", emptyW2);
        assertEquals(AlertState.PENDING, emptyW2.stateAfter());
        writeChecked(engine, labels, 11, T0 + 3 * MINUTE);
        EvaluationRecord w3 = engine.evaluateWindow("cpu-high", labels, T0 + 3 * MINUTE, null, null);
        assertEquals(AlertState.PENDING, w3.stateAfter());
        logEvaluation("ordered-after-fill", w3);
    }
}
