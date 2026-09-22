package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.AlertEvaluator;
import com.github.xingjianluo.metricsalertenginejava.engine.NotificationService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.ComparisonOperator;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.store.AlertStateStore;
import com.github.xingjianluo.metricsalertenginejava.store.NotificationLog;
import com.github.xingjianluo.metricsalertenginejava.store.RuleStore;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 并发求值：同一批 (规则,标签) 窗口被多线程同时驱动，迁移与通知必须守恒。 */
class ConcurrentEvaluationTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentEvaluationTest.class);
    private static final long T0 = 10_000L;
    private static final long W = 1000L;

    @Test
    void concurrentEvaluationAppliesExactlyOncePerKey() throws Exception {
        ManualClock clock = new ManualClock(T0 + 100_000);
        EngineProperties props = TestSupport.properties();
        TimeSeriesStore store = new TimeSeriesStore(props, clock);
        RuleStore ruleStore = new RuleStore();
        AlertStateStore stateStore = new AlertStateStore();
        NotificationLog notificationLog = new NotificationLog();
        AlertEvaluator evaluator = new AlertEvaluator(store, ruleStore, stateStore,
                new NotificationService(notificationLog, clock), notificationLog, clock);

        int hosts = 20;
        for (int h = 0; h < hosts; h++) {
            Labels labels = Labels.of(Map.of("host", "h" + h));
            for (int w = 0; w < 3; w++) {
                store.ingest(new Sample("cpu", labels, 90, T0 + w * W));
            }
        }
        AlertRule rule = ruleStore.register("cpu_high", new RuleStore.RuleSpec(
                "cpu", Map.of(), W, AggregateFunction.AVG, ComparisonOperator.GT, 80.0,
                2, 2, 0));

        int threads = 8;
        int rounds = 4; // 每线程把每个窗口都尝试求值一遍（制造大量重复竞争）
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger nonNullMigrations = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int round = 0; round < rounds; round++) {
                        for (int h = 0; h < hosts; h++) {
                            Labels labels = Labels.of(Map.of("host", "h" + h));
                            for (int w = 0; w < 3; w++) {
                                if (evaluator.evaluateWindow(rule, labels, T0 + w * W) != null) {
                                    nonNullMigrations.incrementAndGet();
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        done.await(30, TimeUnit.SECONDS);
        pool.shutdown();

        int expectedMigrations = hosts * 3; // 每个组合每窗口恰好一次
        long firingNotifications = notificationLog.all().stream()
                .filter(n -> n.type() == com.github.xingjianluo.metricsalertenginejava.model.Notification.Type.FIRING
                        && !n.suppressed()).count();
        long firingStates = stateStore.list(rule.version()).stream()
                .filter(r -> r.state() == AlertState.FIRING).count();
        log.info("[判定] 有效迁移={}/{}（重复竞争被忽略），FIRING 通知={}，FIRING 状态组合={}",
                nonNullMigrations.get(), expectedMigrations, firingNotifications, firingStates);
        assertEquals(expectedMigrations, nonNullMigrations.get(), "每个 (组合,窗口) 必须恰好迁移一次");
        assertEquals(hosts, firingNotifications, "每个标签组合只发一次 FIRING，抖动/并发不重复触发");
        assertEquals(hosts, firingStates, "所有组合都应稳定处于 FIRING");
    }
}
