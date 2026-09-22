package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.AlertEvaluator;
import com.github.xingjianluo.metricsalertenginejava.engine.NotificationService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.AlertEvaluation;
import com.github.xingjianluo.metricsalertenginejava.model.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.ComparisonOperator;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.Notification;
import com.github.xingjianluo.metricsalertenginejava.model.RuleVersion;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.store.AlertStateStore;
import com.github.xingjianluo.metricsalertenginejava.store.NotificationLog;
import com.github.xingjianluo.metricsalertenginejava.store.RuleStore;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 告警状态机：NORMAL→PENDING→FIRING→RECOVERING→NORMAL 的确定语义，
 * 抖动不重复触发，重复窗口求值幂等。
 */
class AlertStateMachineTest {

    private static final Logger log = LoggerFactory.getLogger(AlertStateMachineTest.class);
    private static final long T0 = 10_000L;
    private static final long W = 1000L;

    private ManualClock clock;
    private TimeSeriesStore store;
    private RuleStore ruleStore;
    private AlertStateStore stateStore;
    private NotificationLog notificationLog;
    private AlertEvaluator evaluator;
    private final Labels labels = Labels.of(Map.of("host", "a"));

    @BeforeEach
    void setUp() {
        clock = new ManualClock(T0 + 100_000);
        EngineProperties props = TestSupport.properties();
        store = new TimeSeriesStore(props, clock);
        ruleStore = new RuleStore();
        stateStore = new AlertStateStore();
        notificationLog = new NotificationLog();
        NotificationService notificationService = new NotificationService(notificationLog, clock);
        evaluator = new AlertEvaluator(store, ruleStore, stateStore, notificationService,
                notificationLog, clock);
    }

    /** forThreshold=2, recoveryThreshold=2, 无静默期。 */
    private AlertRule registerRule(long silenceMillis) {
        AlertRule rule = ruleStore.register("cpu_high", new RuleStore.RuleSpec(
                "cpu", Map.of(), W, AggregateFunction.AVG, ComparisonOperator.GT, 80.0,
                2, 2, silenceMillis));
        log.info("[规则] {} metric=cpu AVG>80 for=2 recovery=2 silence={}ms",
                rule.version(), silenceMillis);
        return rule;
    }

    private void put(double value, long ts) {
        WriteResultWrap(store.ingest(new Sample("cpu", labels, value, ts)));
    }

    private void WriteResultWrap(com.github.xingjianluo.metricsalertenginejava.model.WriteResult r) {
        if (!r.accepted()) {
            throw new IllegalStateException("unexpected reject: " + r.reason() + " " + r.detail());
        }
    }

    private AlertEvaluation eval(AlertRule rule, long windowStart) {
        AlertEvaluation e = evaluator.evaluateWindow(rule, labels, windowStart);
        if (e != null) {
            log.info("[求值] window={} value={} met={} 迁移 {}->{} hits={} misses={} 依据={}",
                    e.windowStart(), e.aggregatedValue(), e.conditionMet(), e.fromState(), e.toState(),
                    e.consecutiveHits(), e.consecutiveMisses(), e.reason());
        }
        return e;
    }

    @Test
    void stateTransitionsAreDeterministic() {
        AlertRule rule = registerRule(0);
        put(90, T0);           // w10 满足
        put(90, T0 + 1000);    // w11 满足
        put(10, T0 + 2000);    // w12 不满足
        put(10, T0 + 3000);    // w13 不满足

        assertEquals(AlertState.NORMAL, eval(rule, T0).toState());          // hit 1: 保持 NORMAL
        assertEquals(AlertState.PENDING, eval(rule, T0 + W).toState());     // hit 2: PENDING
        AlertEvaluation firing = eval(rule, T0 + 2 * W);                    // 缺失 w12? 实际有值 10 不满足
        // w12 有值且不满足 -> 恢复计数 1，PENDING 保持
        assertEquals(AlertState.PENDING, firing.toState());
        // 重新满足（抖动恢复）：从 PENDING 再满足一次 -> FIRING
        put(90, T0 + 4000);
        assertEquals(AlertState.FIRING, eval(rule, T0 + 4 * W).toState());

        List<Notification> emitted = evaluator.drainEmittedNotifications();
        log.info("[通知] 到目前实际发出数={}", emitted.size());
        assertEquals(1, emitted.size(), "只应在进入 FIRING 时发一次通知");
        assertEquals(Notification.Type.FIRING, emitted.get(0).type());
    }

    @Test
    void fullCycleNormalPendingFiringRecoveringNormal() {
        AlertRule rule = registerRule(0);
        put(90, T0); put(90, T0 + W);
        eval(rule, T0);
        eval(rule, T0 + W);                 // PENDING
        put(90, T0 + 2 * W);
        assertEquals(AlertState.FIRING, eval(rule, T0 + 2 * W).toState());

        // 连续不满足：第一次保持 FIRING，第二次进 RECOVERING，第三次回 NORMAL
        put(10, T0 + 3 * W);
        put(10, T0 + 4 * W);
        put(10, T0 + 5 * W);
        assertEquals(AlertState.FIRING, eval(rule, T0 + 3 * W).toState());
        assertEquals(AlertState.RECOVERING, eval(rule, T0 + 4 * W).toState());
        AlertEvaluation recovered = eval(rule, T0 + 5 * W);
        assertEquals(AlertState.NORMAL, recovered.toState());

        List<Notification> all = notificationLog.all();
        long firing = all.stream().filter(n -> n.type() == Notification.Type.FIRING && !n.suppressed()).count();
        long recoveredNotes = all.stream().filter(n -> n.type() == Notification.Type.RECOVERED && !n.suppressed()).count();
        log.info("[通知] 完整周期：FIRING={} RECOVERED={}", firing, recoveredNotes);
        assertEquals(1, firing);
        assertEquals(1, recoveredNotes);
    }

    @Test
    void flapDuringRecoveryDoesNotRenotify() {
        AlertRule rule = registerRule(0);
        put(90, T0); put(90, T0 + W); put(90, T0 + 2 * W);
        eval(rule, T0); eval(rule, T0 + W); eval(rule, T0 + 2 * W); // 进入 FIRING
        put(10, T0 + 3 * W); put(10, T0 + 4 * W);
        eval(rule, T0 + 3 * W);
        assertEquals(AlertState.RECOVERING, eval(rule, T0 + 4 * W).toState());

        // 恢复过程中再次满足：回 FIRING，但不发第二次 FIRING 通知
        put(90, T0 + 5 * W);
        assertEquals(AlertState.FIRING, eval(rule, T0 + 5 * W).toState());
        log.info("[判定] RECOVERING 中抖动满足 -> FIRING 但不重复通知，通知总数={}",
                evaluator.drainEmittedNotifications().size());
        assertEquals(0, evaluator.drainEmittedNotifications().size());
    }

    @Test
    void duplicateWindowEvaluationIsIdempotent() {
        AlertRule rule = registerRule(0);
        put(90, T0);
        AlertEvaluation first = eval(rule, T0);
        AlertEvaluation second = eval(rule, T0);
        AlertEvaluation third = eval(rule, T0);
        assertNull(second, "同一窗口重复求值必须被跳过");
        assertNull(third);
        AlertStateStore.StateRecord rec = stateStore.get(rule.version(), labels);
        log.info("[判定] 重复求值 3 次后 hits={} state={}（应仅计一次）", rec.consecutiveHits(), rec.state());
        assertEquals(1, rec.consecutiveHits());
        assertEquals(AlertState.NORMAL, first.toState());
    }

    @Test
    void silenceSuppressesNotificationButKeepsState() {
        AlertRule rule = registerRule(10_000); // 10s 静默
        put(90, T0); put(90, T0 + W); put(90, T0 + 2 * W);
        eval(rule, T0); eval(rule, T0 + W); eval(rule, T0 + 2 * W); // FIRING，发一次
        List<Notification> first = evaluator.drainEmittedNotifications();
        assertEquals(1, first.size());

        // 完整恢复后立即再次触发，落在静默期内：通知被抑制，但状态仍正常迁移
        put(10, T0 + 3 * W); put(10, T0 + 4 * W); put(10, T0 + 5 * W);
        eval(rule, T0 + 3 * W); eval(rule, T0 + 4 * W); eval(rule, T0 + 5 * W);
        put(90, T0 + 6 * W); put(90, T0 + 7 * W); put(90, T0 + 8 * W);
        assertEquals(AlertState.NORMAL, eval(rule, T0 + 6 * W).toState());
        assertEquals(AlertState.PENDING, eval(rule, T0 + 7 * W).toState());
        AlertEvaluation refire = eval(rule, T0 + 8 * W);
        assertEquals(AlertState.FIRING, refire.toState(), "静默只抑制通知，状态必须照常迁移");
        List<Notification> suppressed = notificationLog.all().stream()
                .filter(n -> n.suppressed() && n.type() == Notification.Type.FIRING).toList();
        long suppressedRecovered = notificationLog.all().stream()
                .filter(n -> n.suppressed() && n.type() == Notification.Type.RECOVERED).count();
        log.info("[判定] 静默期内重触发状态={}，被抑制 FIRING={}（另有被抑制 RECOVERED={}）",
                refire.toState(), suppressed.size(), suppressedRecovered);
        assertEquals(1, suppressed.size(), "静默期 FIRING 通知应被抑制并留痕");

        // 时钟越过静默期后走完整恢复 + 再次触发，应真正发出
        clock.advance(11_000);
        put(10, T0 + 9 * W); put(10, T0 + 10 * W); put(10, T0 + 11 * W);
        eval(rule, T0 + 9 * W); eval(rule, T0 + 10 * W); eval(rule, T0 + 11 * W);
        put(90, T0 + 12 * W); put(90, T0 + 13 * W); put(90, T0 + 14 * W);
        eval(rule, T0 + 12 * W); eval(rule, T0 + 13 * W); eval(rule, T0 + 14 * W);
        List<Notification> afterSilence = evaluator.drainEmittedNotifications();
        log.info("[判定] 越过静默期后实际发出通知数={}", afterSilence.size());
        assertEquals(1, afterSilence.size());
    }
}
