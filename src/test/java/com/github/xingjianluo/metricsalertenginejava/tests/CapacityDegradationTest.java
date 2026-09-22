package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.engine.EngineConfig;
import com.github.xingjianluo.metricsalertenginejava.engine.EvalTask;
import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.DropReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.rule.AlertRule;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.LOG;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.MINUTE;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.T0;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.labels;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 容量保护与降级测试：
 * - 单序列点容量超限：最旧点被淘汰（RETENTION_TRIM），计数可观测，窗口查询不再包含被淘汰点；
 * - 异步评估队列有界：队列满时 offer 立即失败并计数 EVAL_QUEUE_FULL，写入路径不被阻塞/拖垮；
 * - 降级丢弃不影响后续同步评估结论的正确性。
 */
class CapacityDegradationTest {

    @Test
    void seriesPointRetentionTrimsOldestWithObservableDropReason() throws Exception {
        EngineConfig config = new EngineConfig(4, 10, 5, 4, 8,
                24 * 60 * MINUTE, MINUTE);
        var holder = TestSupport.engine(config);
        holder.at(T0 + 30 * MINUTE);
        var engine = holder.engine();
        engine.registerMetric("cpu", Set.of("host"), 10, 24 * 60 * MINUTE);
        var labels = labels("host", "a");

        // 连续写 8 个点，容量 5 -> 淘汰 3 个最旧点
        for (int i = 0; i < 8; i++) {
            var r = engine.write(new Sample("cpu", labels, i + 1, T0 + i * MINUTE));
            assertTrue(r.accepted(), r.message());
        }
        long trimmed = engine.dropStats().count(DropReason.RETENTION_TRIM);
        LOG.info("[retention] 写入 8 点 / 容量 5，RETENTION_TRIM={}，证据={}",
                trimmed, engine.dropStats().lastSample(DropReason.RETENTION_TRIM));
        assertEquals(3, trimmed);

        // 被淘汰的是最旧 3 个点（ts=T0,T0+1min,T0+2min）；保留 5 个
        var firstWindow = engine.queryWindow("cpu", labels, T0, MINUTE, null,
                java.util.List.of(Aggregator.COUNT));
        var thirdWindow = engine.queryWindow("cpu", labels, T0 + 2 * MINUTE, MINUTE, null,
                java.util.List.of(Aggregator.COUNT));
        var keptWindow = engine.queryWindow("cpu", labels, T0 + 3 * MINUTE, MINUTE, null,
                java.util.List.of(Aggregator.COUNT, Aggregator.SUM));
        LOG.info("[retention] 被淘汰窗口 count={},{} 保留窗口 {}",
                firstWindow.count(), thirdWindow.count(), keptWindow);
        assertEquals(0L, firstWindow.count());
        assertEquals(0L, thirdWindow.count());
        assertEquals(1L, keptWindow.count());
        assertEquals(4.0, keptWindow.get(Aggregator.SUM));
    }

    @Test
    void evalQueueFullDropsFastAndNeverBlocksWrites() {
        EngineConfig config = new EngineConfig(4, 10, 100, 4, 4,
                24 * 60 * MINUTE, MINUTE);
        var holder = TestSupport.engine(config);
        holder.at(T0 + 30 * MINUTE);
        var engine = holder.engine();
        engine.registerMetric("cpu", Set.of("host"), 10, 24 * 60 * MINUTE);
        engine.registerRule("r", "cpu", Aggregator.SUM, AlertRule.Operator.GT, 10.0,
                1, 1, MINUTE, 0, Set.of());
        var labels = labels("host", "a");
        var rule = engine.ruleRegistry().current("r");

        // 填满容量为 4 的队列（不消费）
        for (int i = 0; i < 4; i++) {
            assertTrue(engine.offerAsyncEval(new EvalTask(rule, labels, T0 + i * MINUTE, System.nanoTime())));
        }
        // 第 5 个入队立即失败、快速计数，不能阻塞
        long startNanos = System.nanoTime();
        boolean rejected = engine.offerAsyncEval(
                new EvalTask(rule, labels, T0 + 4 * MINUTE, System.nanoTime()));
        long elapsedMicros = (System.nanoTime() - startNanos) / 1000;
        assertFalse(rejected);
        LOG.info("[eval-queue] 队列满后 offer 立即返回 false，耗时 {} 微秒；EVAL_QUEUE_FULL={}",
                elapsedMicros, engine.dropStats().count(DropReason.EVAL_QUEUE_FULL));
        assertTrue(elapsedMicros < 50_000, "入队失败必须快速返回，不得阻塞写入路径");
        assertEquals(1, engine.dropStats().count(DropReason.EVAL_QUEUE_FULL));
        assertEquals(4, engine.evalQueueSize());

        // 丢弃评估任务不影响数据：消费过慢期间写入照常全部成功
        for (int i = 0; i < 20; i++) {
            var wr = engine.write(new Sample("cpu", labels, 100, T0 + (10 + i) * MINUTE));
            assertTrue(wr.accepted(), wr.message());
        }
        LOG.info("[eval-queue] 队列积压期间 20 次写入全部 accepted，seriesCount={}",
                engine.seriesCount("cpu"));

        // 排空队列后，同步评估仍能得出正确结论（降级只丢异步任务，不丢数据、不改状态）
        while (engine.pollAsyncEval() != null) {
            // drain
        }
        for (int w = 0; w < 3; w++) {
            engine.write(new Sample("cpu", labels, 100, T0 + w * MINUTE));
        }
        engine.evaluateRange("r", labels, T0, T0 + 3 * MINUTE, null, null);
        assertEquals(AlertState.FIRING, engine.alertState("r", null, labels));
        LOG.info("[eval-queue] 排空后同步评估状态={}（结论不受队列丢弃影响）",
                engine.alertState("r", null, labels));
    }

    @Test
    void metricAndRuleCapacityHardLimitsRejectWithDistinctErrors() {
        EngineConfig config = new EngineConfig(2, 10, 100, 2, 4,
                24 * 60 * MINUTE, MINUTE);
        var holder = TestSupport.engine(config);
        holder.at(T0);
        var engine = holder.engine();
        engine.registerMetric("m1", Set.of(), 10, 24 * 60 * MINUTE);
        engine.registerMetric("m2", Set.of(), 10, 24 * 60 * MINUTE);
        var ex = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> engine.registerMetric("m3", Set.of(), 10, 24 * 60 * MINUTE));
        LOG.info("[capacity] 第三个指标被拒绝: {}", ex.getMessage());
        assertTrue(ex.getMessage().contains("metric.capacity.exceeded"));

        engine.registerRule("r1", "m1", Aggregator.COUNT, AlertRule.Operator.GT, 0.0,
                1, 1, MINUTE, 0, Set.of());
        engine.registerRule("r2", "m1", Aggregator.COUNT, AlertRule.Operator.GT, 0.0,
                1, 1, MINUTE, 0, Set.of());
        var ex2 = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> engine.registerRule("r3", "m1", Aggregator.COUNT, AlertRule.Operator.GT, 0.0,
                        1, 1, MINUTE, 0, Set.of()));
        LOG.info("[capacity] 第三条规则被拒绝: {}", ex2.getMessage());
        assertTrue(ex2.getMessage().contains("rule.capacity.exceeded"));
        assertNull(engine.metric("m3"));
    }
}
