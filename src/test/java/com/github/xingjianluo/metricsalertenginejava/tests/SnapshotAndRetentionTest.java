package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.QueryService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.SeriesAggregation;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并发写查下快照自洽（窗口计数等于可枚举点数），以及保留期淘汰。 */
class SnapshotAndRetentionTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotAndRetentionTest.class);

    @Test
    void concurrentReadSnapshotIsAlwaysSelfConsistent() throws Exception {
        ManualClock clock = new ManualClock(0);
        EngineProperties props = TestSupport.properties();
        props.setRetentionMillis(Long.MAX_VALUE / 2);
        props.setMaxSeriesPerMetric(100_000);
        props.setMaxTotalSeries(100_000);
        props.setMaxPointsPerSeries(1_000_000);
        TimeSeriesStore store = new TimeSeriesStore(props, clock);
        QueryService query = new QueryService(store);

        int writers = 4;
        int perWriter = 2_000;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger checked = new AtomicInteger();
        Thread reader = new Thread(() -> {
            while (!stop.get()) {
                List<SeriesAggregation> result = query.query("m", Map.of(), 0, 60_000, 1000,
                        AggregateFunction.COUNT);
                // 自洽不变量：每个快照中窗口 sampleCount 之和必为非负，且不超过当前总点数
                long sum = 0;
                for (SeriesAggregation s : result) {
                    for (SeriesAggregation.WindowValue w : s.windows()) {
                        assertTrue(w.sampleCount() >= 0);
                        sum += w.sampleCount();
                    }
                }
                assertTrue(sum <= store.totalSampleCount(),
                        "快照计数 " + sum + " 不得超过已落库点数 " + store.totalSampleCount());
                checked.incrementAndGet();
            }
        });
        reader.start();

        for (int t = 0; t < writers; t++) {
            final int tid = t;
            new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        Labels labels = Labels.of(Map.of("w", String.valueOf(tid)));
                        WriteResult r = store.ingest(new Sample("m", labels, 1, i * 10L));
                        assertTrue(r.accepted(), "重复时间戳不应出现: " + r.detail());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }
        start.countDown();
        done.await(30, java.util.concurrent.TimeUnit.SECONDS);
        stop.set(true);
        reader.join(5000);

        long expected = (long) writers * perWriter;
        log.info("[判定] 并发写入 {} 点全部落库 totalSamples={}，读侧自洽校验次数={}",
                expected, store.totalSampleCount(), checked.get());
        assertEquals(expected, store.totalSampleCount(), "写入点数守恒");
        assertTrue(checked.get() > 0);
    }

    @Test
    void retentionEvictsOldSamplesAndEmptySeries() {
        ManualClock clock = new ManualClock(100_000);
        EngineProperties props = TestSupport.properties();
        props.setRetentionMillis(10_000);
        TimeSeriesStore store = new TimeSeriesStore(props, clock);
        Labels a = Labels.of(Map.of("h", "a"));
        Labels b = Labels.of(Map.of("h", "b"));
        // 89999 相对淘汰时刻(now=100000,cutoff=90000)是旧点；写入时把时钟设在 99998（下界 89998）
        assertTrue(store.ingest(new Sample("m", a, 1, 95_000)).accepted()); // 保留
        assertTrue(store.ingest(new Sample("m", a, 2, 90_000)).accepted()); // 界上保留
        clock.set(99_998);
        assertTrue(store.ingest(new Sample("m", a, 3, 89_998)).accepted()); // 旧点，淘汰
        assertTrue(store.ingest(new Sample("m", b, 1, 89_999)).accepted()); // 旧且唯一
        clock.set(100_000);
        int removed = store.evict(clock.nowMillis());
        log.info("[判定] now=100000 retention=10000，淘汰点数={}，剩余总点数={}，剩余时间线={}（全空时间线被清理）",
                removed, store.totalSampleCount(), store.totalSeriesCount());
        assertEquals(2, removed);
        assertEquals(2, store.totalSampleCount());
        assertEquals(1, store.totalSeriesCount(), "b 时间线清空后应被移除");
    }
}
