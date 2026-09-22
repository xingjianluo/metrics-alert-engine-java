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
import static org.junit.jupiter.api.Assertions.assertNotSame;

/** 规则版本化：旧版本不可变、历史结论按版本可复现、新版本状态独立。 */
class RuleVersioningTest {

    private static final Logger log = LoggerFactory.getLogger(RuleVersioningTest.class);
    private static final long T0 = 10_000L;
    private static final long W = 1000L;

    private ManualClock clock;
    private TimeSeriesStore store;
    private RuleStore ruleStore;
    private AlertStateStore stateStore;
    private AlertEvaluator evaluator;

    @BeforeEach
    void setUp() {
        clock = new ManualClock(T0 + 100_000);
        EngineProperties props = TestSupport.properties();
        store = new TimeSeriesStore(props, clock);
        ruleStore = new RuleStore();
        stateStore = new AlertStateStore();
        NotificationLog log = new NotificationLog();
        evaluator = new AlertEvaluator(store, ruleStore, stateStore,
                new NotificationService(log, clock), log, clock);
    }

    private void put(double v, long ts) {
        WriteResultWrap(store.ingest(new Sample("cpu", Labels.of(Map.of("host", "a")), v, ts)));
    }

    private void WriteResultWrap(com.github.xingjianluo.metricsalertenginejava.model.WriteResult r) {
        if (!r.accepted()) {
            throw new IllegalStateException(r.reason() + " " + r.detail());
        }
    }

    @Test
    void updateCreatesNewVersionAndHistoryIsReproducible() {
        AlertRule v1 = ruleStore.register("cpu_high", new RuleStore.RuleSpec(
                "cpu", Map.of(), W, AggregateFunction.AVG, ComparisonOperator.GT, 80.0,
                1, 1, 0));
        // v1: for=1 -> hit 1 PENDING, hit 2 FIRING
        put(90, T0); put(90, T0 + W);
        evaluator.evaluateWindow(v1, Labels.of(Map.of("host", "a")), T0);
        evaluator.evaluateWindow(v1, Labels.of(Map.of("host", "a")), T0 + W);
        AlertState v1StateAfter = stateStore.get(v1.version(), Labels.of(Map.of("host", "a"))).state();
        log.info("[v1] 阈值 80 for=1：两窗口后状态={}", v1StateAfter);
        assertEquals(AlertState.FIRING, v1StateAfter);

        // 更新阈值与阈值次数，产生 v2；v1 记录不可变
        AlertRule v2 = ruleStore.update("cpu_high", new RuleStore.RuleSpec(
                "cpu", Map.of(), W, AggregateFunction.AVG, ComparisonOperator.GT, 95.0,
                2, 2, 0));
        log.info("[更新] 规则升级到 {}（阈值 95 for=2），v1 仍保留：{}", v2.version(),
                ruleStore.versions("cpu_high").stream().map(AlertRule::version).toList());
        assertEquals(2, v2.version().version());
        assertEquals(2, ruleStore.versions("cpu_high").size());

        // v2 对相同数据（90<95）应保持 NORMAL；v1 状态依旧是 FIRING，互不影响
        evaluator.evaluateWindow(v2, Labels.of(Map.of("host", "a")), T0);
        evaluator.evaluateWindow(v2, Labels.of(Map.of("host", "a")), T0 + W);
        AlertState v2State = stateStore.get(v2.version(), Labels.of(Map.of("host", "a"))).state();
        AlertState v1StateRepro = stateStore.get(v1.version(), Labels.of(Map.of("host", "a"))).state();
        log.info("[判定] v2 状态={}（90 不满足 95），v1 复现状态={}（两版本状态隔离）", v2State, v1StateRepro);
        assertEquals(AlertState.NORMAL, v2State);
        assertEquals(AlertState.FIRING, v1StateRepro);

        // 历史结论按版本逐条复现且追加顺序确定
        List<AlertEvaluation> v1History = stateStore.history(v1.version());
        List<AlertEvaluation> v2History = stateStore.history(v2.version());
        log.info("[复现] v1 历史条数={} 迁移序列={}", v1History.size(),
                v1History.stream().map(e -> e.fromState() + "->" + e.toState()).toList());
        assertEquals(2, v1History.size());
        assertEquals(2, v2History.size());
        assertNotSame(v1.version(), v2.version());
    }

    @Test
    void evaluateSpecificVersionUsesThatVersionsSemantics() {
        AlertRule v1 = ruleStore.register("cpu_high", new RuleStore.RuleSpec(
                "cpu", Map.of(), W, AggregateFunction.AVG, ComparisonOperator.LT, 50.0,
                1, 1, 0));
        AlertRule v2 = ruleStore.update("cpu_high", new RuleStore.RuleSpec(
                "cpu", Map.of(), W, AggregateFunction.AVG, ComparisonOperator.LT, 5.0,
                1, 1, 0));
        put(30, T0);
        // 取 v1（阈值 50）求值应 PENDING；取 v2（阈值 5）求值应 NORMAL
        List<AlertEvaluation> r1 = evaluator.evaluateRule(ruleStore.get(new RuleVersion("cpu_high", 1)), T0);
        List<AlertEvaluation> r2 = evaluator.evaluateRule(ruleStore.get(new RuleVersion("cpu_high", 2)), T0);
        log.info("[判定] 值 30：v1(LT50)->{}，v2(LT5)->{}（按选定版本语义复现）",
                r1.get(0).toState(), r2.get(0).toState());
        assertEquals(AlertState.PENDING, r1.get(0).toState());
        assertEquals(AlertState.NORMAL, r2.get(0).toState());
    }
}
