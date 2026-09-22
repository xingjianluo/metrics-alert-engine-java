package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.engine.EngineConfig;
import com.github.xingjianluo.metricsalertenginejava.engine.MetricsAlertEngine;
import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.LOG;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.MINUTE;
import static com.github.xingjianluo.metricsalertenginejava.tests.TestSupport.T0;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发写入与查询一致性测试：
 * - 多线程并发写相同时间戳：accepted（含重复覆盖）次数守恒，最终唯一点数确定；
 * - 基数上限在并发下严格生效，超额拒绝全部归因 CARDINALITY_EXCEEDED；
 * - 并发查询拿到的窗口快照始终自洽（COUNT 等于参与 SUM 的点数，且不超最终落库数）。
 */
class ConcurrentWriteTest {

    @Test
    void concurrentWritesToSameTimestampsConserveUniquePoints() throws Exception {
        var holder = TestSupport.defaultEngine();
        holder.at(T0 + 240 * MINUTE); // 覆盖数据范围 T0..T0+200min
        var engine = holder.engine();
        engine.registerMetric("cpu", Set.of("host"), 100, 24 * 60 * MINUTE);
        LabelSet labels = TestSupport.labels("host", "a");

        int threads = 8;
        int perThread = 500;
        int distinctTimestamps = 200; // 8 个线程并发覆盖同样的 200 个时间戳
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    long ts = T0 + (i % distinctTimestamps) * MINUTE;
                    double value = tid * perThread + i;
                    WriteResult r = engine.write(new Sample("cpu", labels, value, ts));
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                        if (r.duplicate()) {
                            duplicates.incrementAndGet();
                        }
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        int totalAttempts = threads * perThread;
        LOG.info("[concurrent-same-ts] attempts={} accepted={} duplicates={} uniqueExpected={}",
                totalAttempts, accepted.get(), duplicates.get(), distinctTimestamps);
        assertEquals(totalAttempts, accepted.get(), "全部时间戳合法，必须全部 accepted");
        // 每个时间戳首个写入非重复，其余均为重复覆盖
        assertEquals(totalAttempts - distinctTimestamps, duplicates.get());

        // 守恒：200 个窗口各恰好 1 个点；总计 200 个唯一点
        long totalPoints = 0;
        for (int w = 0; w < distinctTimestamps; w++) {
            var agg = engine.queryWindow("cpu", labels, T0 + w * MINUTE, MINUTE, null,
                    List.of(Aggregator.COUNT));
            assertEquals(1L, agg.count(), "window " + w + " must contain exactly 1 point");
            totalPoints += agg.count();
        }
        assertEquals(distinctTimestamps, totalPoints);
        LOG.info("[concurrent-same-ts] 最终唯一点数={} 守恒", totalPoints);
    }

    @Test
    void concurrentDistinctSeriesObeyCardinalityLimitStrictly() throws Exception {
        EngineConfig config = new EngineConfig(4, 50, 1000, 4, 8,
                24 * 60 * MINUTE, MINUTE);
        var holder = TestSupport.engine(config);
        holder.at(T0 + 60 * MINUTE);
        var engine = holder.engine();
        final int cardinalityLimit = 50;
        engine.registerMetric("cpu", Set.of("host"), cardinalityLimit, 24 * 60 * MINUTE);

        int threads = 8;
        int perThread = 20; // 160 个不同 host，远大于 50
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    int hostId = tid * perThread + i;
                    LabelSet labels = TestSupport.labels("host", "h" + hostId);
                    WriteResult r = engine.write(new Sample("cpu", labels, 1, T0));
                    if (r.accepted()) {
                        ok.incrementAndGet();
                    } else {
                        assertEquals(RejectReason.CARDINALITY_EXCEEDED, r.reason());
                        rejected.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        LOG.info("[concurrent-cardinality] accepted={} rejected={} limit={} seriesCount={}",
                ok.get(), rejected.get(), cardinalityLimit, engine.seriesCount("cpu"));
        assertEquals(cardinalityLimit, ok.get(), "成功数必须严格等于基数上限");
        assertEquals(threads * perThread - cardinalityLimit, rejected.get());
        assertEquals(cardinalityLimit, engine.seriesCount("cpu"));
    }

    @Test
    void concurrentQueriesAlwaysSeeSelfConsistentSnapshots() throws Exception {
        var holder = TestSupport.defaultEngine();
        holder.at(T0 + 240 * MINUTE); // 覆盖数据范围 T0..T0+200min
        var engine = holder.engine();
        engine.registerMetric("cpu", Set.of("host"), 100, 24 * 60 * MINUTE);
        LabelSet labels = TestSupport.labels("host", "a");

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger observedSnapshots = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        // 2 个写线程：每窗口写 0..N 的若干点
        for (int t = 0; t < 2; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int w = 0; w < 100; w++) {
                    for (int p = 0; p < 10; p++) {
                        // 两线程使用窗口内不重叠的秒段 [0,10s) 与 [20s,30s)，每窗口恰好 20 个唯一点
                        long offset = (tid == 0 ? 0L : 20_000L) + p * 1000L;
                        engine.write(new Sample("cpu", labels, 1, T0 + w * MINUTE + offset));
                    }
                }
                return null;
            }));
        }
        // 2 个只读线程：反复查询，验证快照自洽——同一窗口 COUNT 单调不减，且 COUNT<=20
        for (int q = 0; q < 2; q++) {
            futures.add(pool.submit(() -> {
                start.await();
                long[] lastCount = new long[100];
                for (int round = 0; round < 200; round++) {
                    for (int w = 0; w < 100; w++) {
                        var agg = engine.queryWindow("cpu", labels, T0 + w * MINUTE, MINUTE, null,
                                List.of(Aggregator.COUNT, Aggregator.SUM));
                        long count = agg.count();
                        double sum = agg.get(Aggregator.SUM);
                        assertTrue(count <= 20, "COUNT 不得超过最终总数 20: " + count);
                        assertTrue(count >= lastCount[w], "同一窗口 COUNT 不得随时间倒退（快照自洽）");
                        // SUM==COUNT：每个点值都是 1，COUNT 与 SUM 必须在同一快照下一致
                        assertEquals((double) count, sum, 0.0001,
                                "同一快照内 COUNT 与 SUM 必须相互自洽");
                        lastCount[w] = count;
                    }
                    observedSnapshots.incrementAndGet();
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        // 最终每个窗口 20 个点（2 线程 x 10 点；时间戳互不重叠）
        for (int w = 0; w < 100; w++) {
            var agg = engine.queryWindow("cpu", labels, T0 + w * MINUTE, MINUTE, null,
                    List.of(Aggregator.COUNT));
            assertEquals(20L, agg.count());
        }
        LOG.info("[concurrent-snapshot] 读者完成 {} 轮快照，全部自洽；最终 100 窗口 x 20 点守恒",
                observedSnapshots.get());
    }
}
