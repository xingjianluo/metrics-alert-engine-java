package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.IngestionService;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 写入入口：入队前校验、队列满降级、关闭拒绝。 */
class IngestionServiceTest {

    private static final Logger log = LoggerFactory.getLogger(IngestionServiceTest.class);
    private static final long T0 = 1_000_000L;

    @Test
    void invalidLabelsAndTimestampsRejectedBeforeQueue() throws Exception {
        ManualClock clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        MetricsRegistry registry = new MetricsRegistry();
        try (IngestionService svc = new IngestionService(new TimeSeriesStore(props, clock),
                registry, props, clock)) {
            svc.start();
            WriteResult badLabel = svc.submit("m", Map.of("1bad", "v"), 1, T0).get(1, TimeUnit.SECONDS);
            WriteResult badTs = svc.submit("m", Map.of("h", "a"), 1, T0 - props.getRetentionMillis() - 1)
                    .get(1, TimeUnit.SECONDS);
            assertFalse(badLabel.accepted());
            assertEquals(RejectReason.INVALID_LABEL, badLabel.reason());
            assertEquals(RejectReason.TIMESTAMP_OUT_OF_RANGE, badTs.reason());
            log.info("[判定] 非法标签={} 越界时间戳={}，且 queueDepth={}（均未入队）",
                    badLabel.reason(), badTs.reason(), svc.queueDepth());
            assertEquals(0, svc.queueDepth());
            assertEquals(1L, registry.rejectedCounts().get(RejectReason.INVALID_LABEL));
            assertEquals(1L, registry.rejectedCounts().get(RejectReason.TIMESTAMP_OUT_OF_RANGE));
        }
    }

    @Test
    void queueFullDegradesWithDistinctReasonAndNeverBlocksCallers() throws Exception {
        ManualClock clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        props.setIngestQueueCapacity(4);
        // 用锁存器把 worker 卡在第一条消息处理上，制造消费过慢。
        CountDownLatch storeGate = new CountDownLatch(1);
        TimeSeriesStore blockingStore = new TimeSeriesStore(props, clock) {
            @Override
            public WriteResult ingest(com.github.xingjianluo.metricsalertenginejava.model.Sample sample) {
                try {
                    storeGate.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.ingest(sample);
            }
        };
        MetricsRegistry registry = new MetricsRegistry();
        try (IngestionService svc = new IngestionService(blockingStore, registry, props, clock)) {
            svc.start();
            List<CompletableFuture<WriteResult>> futures = new ArrayList<>();
            int queueFull = 0;
            for (int i = 0; i < 20; i++) {
                CompletableFuture<WriteResult> f = svc.submit("m", Labels.of(Map.of("h", String.valueOf(i))),
                        i, T0 + i);
                futures.add(f);
                WriteResult immediate = f.getNow(null);
                if (immediate != null && !immediate.accepted()
                        && immediate.reason() == RejectReason.QUEUE_FULL) {
                    queueFull++;
                }
            }
            long maxDepth = svc.queueDepth();
            log.info("[判定] 消费阻塞期间 QUEUE_FULL 次数={}，峰值队列深度={}（容量 4）", queueFull, maxDepth);
            assertTrue(queueFull > 0, "消费过慢必须出现 QUEUE_FULL 降级");
            assertTrue(maxDepth <= 4, "队列深度不得超过容量");
            assertEquals(queueFull, registry.rejectedCounts().get(RejectReason.QUEUE_FULL),
                    "QUEUE_FULL 计数必须守恒、可观测");
            storeGate.countDown();
            for (CompletableFuture<WriteResult> f : futures) {
                WriteResult r = f.get(2, TimeUnit.SECONDS);
                assertTrue(r.accepted() || r.reason() == RejectReason.QUEUE_FULL);
            }
        }
    }

    @Test
    void concurrentWritesConserveCounts() throws Exception {
        ManualClock clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        props.setIngestQueueCapacity(5000);
        props.setMaxSeriesPerMetric(100_000);
        props.setMaxTotalSeries(100_000);
        TimeSeriesStore store = new TimeSeriesStore(props, clock);
        MetricsRegistry registry = new MetricsRegistry();
        int threads = 8;
        int perThread = 500;
        try (IngestionService svc = new IngestionService(store, registry, props, clock)) {
            svc.start();
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch done = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < perThread; i++) {
                            Labels labels = Labels.of(Map.of("thread", String.valueOf(tid), "i", String.valueOf(i)));
                            svc.submitAndWait("m", labels, i, T0 + i);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, TimeUnit.SECONDS));
            pool.shutdown();
        }
        log.info("[判定] 并发 {} 线程 x {} 写入：accepted={} 总采样={} 时间线={}",
                threads, perThread, registry.acceptedCount(),
                store.totalSampleCount(), store.totalSeriesCount());
        assertEquals((long) threads * perThread, registry.acceptedCount(), "接受数必须守恒");
        assertEquals((long) threads * perThread, store.totalSampleCount(), "落库采样数必须守恒");
        assertEquals(threads * perThread, store.totalSeriesCount());
    }
}
