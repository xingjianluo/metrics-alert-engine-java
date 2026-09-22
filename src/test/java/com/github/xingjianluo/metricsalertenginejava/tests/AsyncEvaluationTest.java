package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.engine.EngineConfig;
import com.github.xingjianluo.metricsalertenginejava.engine.EvalTask;
import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.DropReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.rule.AlertRule;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.LOG;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.MINUTE;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.T0;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.labels;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 异步评估端到端测试：
 * 生产者把评估任务投入有界队列，后台单消费者串行处理（保证状态按窗口顺序推进）；
 * 当消费者被人为阻塞（消费过慢）时，队列满，后续 offer 快速失败计数，写入线程不受影响；
 * 恢复消费后，已入队任务全部处理完毕，状态结论与纯同步一致。
 */
class AsyncEvaluationTest {

    @Test
    void slowConsumerDropsQueueTasksButWritesSurviveAndStateStaysCorrect() throws Exception {
        EngineConfig config = new EngineConfig(4, 10, 1000, 4, 16,
                24 * 60 * MINUTE, MINUTE);
        var holder = TestSupport.engine(config);
        holder.at(T0 + 120 * MINUTE);
        var engine = holder.engine();
        engine.registerMetric("cpu", java.util.Set.of("host"), 10, 24 * 60 * MINUTE);
        engine.registerRule("r", "cpu", Aggregator.SUM, AlertRule.Operator.GT, 10.0,
                2, 2, MINUTE, 0, java.util.Set.of());
        var labels = labels("host", "a");
        var rule = engine.ruleRegistry().current("r");

        // 先写入全部数据（6 个窗口，前 4 个命中，后 2 个未命中）
        for (int w = 0; w < 6; w++) {
            double v = w < 4 ? 20 : 0;
            var wr = engine.write(new Sample("cpu", labels, v, T0 + w * MINUTE + 1000));
            assertTrue(wr.accepted(), wr.message());
        }

        CountDownLatch consumerPaused = new CountDownLatch(1);
        CountDownLatch resumeConsumer = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        ExecutorService consumerPool = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "slow-eval-consumer");
            t.setDaemon(true);
            return t;
        });

        // 后台消费者：处理第一个任务前阻塞，模拟消费过慢
        consumerPool.submit(() -> {
            boolean first = true;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    EvalTask task = engine.pollAsyncEval();
                    if (task == null) {
                        Thread.sleep(2);
                        continue;
                    }
                    if (first) {
                        first = false;
                        consumerPaused.countDown();
                        resumeConsumer.await();
                    }
                    // 串行处理：把 [0, toWindow] 的窗口按顺序同步评估（evaluateThrough 自动续接）
                    engine.evaluateThrough("r", labels, task.toWindowStart(), task.rule().version());
                    processed.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // 投递远超过队列容量 16 的任务，消费者卡在第一个任务上
        int offeredOk = 0;
        int dropped = 0;
        for (int w = 0; w < 50; w++) {
            EvalTask task = new EvalTask(rule, labels, T0 + w * MINUTE, System.nanoTime());
            if (engine.offerAsyncEval(task)) {
                offeredOk++;
            } else {
                dropped++;
            }
            if (w == 0) {
                consumerPaused.await(5, TimeUnit.SECONDS);
            }
        }
        LOG.info("[slow-consumer] 投递 50 任务（消费者阻塞）：入队={} 丢弃={} EVAL_QUEUE_FULL={}",
                offeredOk, dropped, engine.dropStats().count(DropReason.EVAL_QUEUE_FULL));
        // 消费者已取走 1 个任务（在队列外持有并阻塞），队列内最多再容纳 16 个
        assertTrue(offeredOk <= 17, "消费者持 1 + 队列容量 16，offer 成功数最多 17，实际=" + offeredOk);
        assertTrue(offeredOk >= 16, "至少应填满有界队列，实际=" + offeredOk);
        assertEquals(50 - offeredOk, dropped);
        assertEquals(dropped, engine.dropStats().count(DropReason.EVAL_QUEUE_FULL));

        // 恢复消费，等待已入队任务处理完
        resumeConsumer.countDown();
        long deadline = System.currentTimeMillis() + 10_000;
        while (processed.get() < offeredOk && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        consumerPool.shutdownNow();
        assertEquals(offeredOk, processed.get(), "已入队任务最终必须全部处理完毕");

        // 状态正确性：已处理窗口覆盖 w0..w(offeredOk-1)，前 4 个命中(pending=2)
        // 若至少处理到 w3，则必然 FIRING；断言结论与同步语义一致
        var history = engine.evaluationHistory("r", 1, labels);
        LOG.info("[slow-consumer] 已处理 {} 个窗口，最新状态={}，丢弃未影响数据与已入队处理",
                history.size(), engine.alertState("r", 1, labels));
        assertEquals(offeredOk, history.size());
        // 同步补齐剩余窗口到 w5，完整生命周期：FIRING -> 连续两窗未命中 -> RESOLVED
        if (history.size() < 6) {
            engine.evaluateThrough("r", labels, T0 + 5 * MINUTE, 1);
        }
        LOG.info("[slow-consumer] 补齐评估后最终状态={}", engine.alertState("r", 1, labels));
        assertEquals(com.github.xingjianluo.metricsalertenginejava.model.AlertState.RESOLVED,
                engine.alertState("r", 1, labels));
    }
}
