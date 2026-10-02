package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.IngestionService;
import com.github.xingjianluo.metricsalertenginejava.engine.MetricDefinitionService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 定义更新与并发写入/查询交错：口径版本自洽、接受与拒绝计数守恒、并发更新版本号唯一连续。 */
class DefinitionConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(DefinitionConcurrencyTest.class);
    private static final long T0 = 1_000_000L;

    private ManualClock clock;
    private EngineProperties props;
    private TimeSeriesStore store;
    private MetricDefinitionStore defStore;
    private MetricsRegistry registry;
    private MetricDefinitionService definitions;
    private IngestionService ingestion;

    @BeforeEach
    void setUp() {
        clock = new ManualClock(T0 + 100_000);
        props = TestSupport.properties();
        // 大队列避免 QUEUE_FULL 干扰本用例（语义口径计数守恒），队列满由既有 IngestionServiceTest 覆盖。
        props.setIngestQueueCapacity(200_000);
        // 本用例需要 400 个并存版本，放开每指标版本上限（上限拒绝路径由 MetricDefinitionTest 专门覆盖）。
        props.setMaxDefinitionVersionsPerMetric(2_000);
        store = new TimeSeriesStore(props, clock);
        defStore = new MetricDefinitionStore();
        registry = new MetricsRegistry();
        definitions = new MetricDefinitionService(defStore, registry, props, clock);
        ingestion = new IngestionService(store, registry, props, clock, definitions);
        ingestion.start();
    }

    @AfterEach
    void tearDown() {
        ingestion.close();
    }

    @Test
    void interleavedUpdatesAndWritesKeepCountsConsistent() throws Exception {
        // v1：范围 [0,100]，白名单 [host]。
        definitions.register("cpu", new MetricDefinition.Spec("%", "v1",
                AggregateFunction.AVG, 0.0, 100.0, Set.of("host")));

        int threads = 4;
        int perThread = 800;
        int total = threads * perThread;
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        CyclicBarrier barrier = new CyclicBarrier(threads + 1);
        ConcurrentLinkedQueue<WriteResult> results = new ConcurrentLinkedQueue<>();
        AtomicInteger done = new AtomicInteger();

        // 写入线程：每条时间戳唯一（线程编号与序号编码），值在 0/50/150 间循环，标签在 host/rack 间循环。
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (int i = 0; i < perThread; i++) {
                    double value = (i % 3 == 0) ? 150.0 : 50.0;
                    // rack 取小集合避免时间线基数爆炸；只用于触发“标签键是否在白名单”的判定。
                    Map<String, String> labels = (i % 2 == 0)
                            ? Map.of("host", "h" + threadId)
                            : Map.of("host", "h" + threadId, "rack", "r" + (i % 4));
                    // 时间戳落在当前时刻之前、保留窗口之内；threadId*1000+i 保证全局唯一不撞点。
                    long ts = clock.nowMillis() - (threadId * 1000L + i);
                    results.add(ingestion.submit("cpu", labels, value, ts).join());
                }
                done.incrementAndGet();
            });
        }
        // 更新线程：写入进行中多次改口径（范围与白名单交替放宽/收紧），版本号持续增长。
        pool.submit(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            for (int v = 2; v <= 6; v++) {
                try {
                    Set<String> keys = (v % 2 == 0) ? Set.of("host", "rack") : Set.of("host");
                    Double max = (v % 2 == 0) ? 200.0 : 100.0;
                    definitions.update("cpu", new MetricDefinition.Spec("%", "v" + v,
                            AggregateFunction.AVG, 0.0, max, keys));
                    log.info("[交错] 定义更新到 v{}：max={} 白名单={}", v, max, keys);
                } catch (RuntimeException e) {
                    log.warn("[交错] 更新被拒绝：{}", e.getMessage());
                }
                try {
                    Thread.sleep(2L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        // 排空 worker。
        long deadline = System.currentTimeMillis() + 10_000;
        while (results.size() < total && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(total, results.size(), "所有写入都必须有明确结论，不能丢数");

        Map<Boolean, Long> byAccepted = results.stream()
                .collect(Collectors.partitioningBy(WriteResult::accepted, Collectors.counting()));
        long accepted = byAccepted.get(true);
        long rejected = byAccepted.get(false);
        log.info("[汇总] 总写入={} 接受={} 拒绝={}（v1..v6 与写入交错）", total, accepted, rejected);

        // 每条结论都必须携带其校验时看到的定义版本（cpu 全程已登记，不可能为 null）。
        for (WriteResult r : results) {
            assertTrue(r.definitionVersion() != null
                    && r.definitionVersion().metric().equals("cpu"), "每条结论必须带上定义版本");
            assertTrue(r.definitionVersion().version() >= 1
                    && r.definitionVersion().version() <= 6, "版本必须是交错期间存在的版本");
        }
        Map<Integer, Long> acceptedByVersion = results.stream()
                .filter(WriteResult::accepted)
                .collect(Collectors.groupingBy(r -> r.definitionVersion().version(), Collectors.counting()));
        Map<Integer, Long> rejectedByVersion = results.stream()
                .filter(r -> !r.accepted())
                .collect(Collectors.groupingBy(r -> r.definitionVersion().version(), Collectors.counting()));
        log.info("[按版本] 接受={} 拒绝={}（每个版本的接受+拒绝=该版本期间判定的写入数）",
                acceptedByVersion, rejectedByVersion);

        // 拒绝原因只能来自定义口径两类（队列大、时间戳合法、标签格式合法）。
        Map<RejectReason, Long> rejectReasons = results.stream()
                .filter(r -> !r.accepted())
                .collect(Collectors.groupingBy(WriteResult::reason, Collectors.counting()));
        log.info("[拒绝原因分布] {}", rejectReasons);
        for (RejectReason reason : rejectReasons.keySet()) {
            assertTrue(reason == RejectReason.DEFINITION_VALUE_OUT_OF_RANGE
                    || reason == RejectReason.DEFINITION_LABEL_KEY_NOT_ALLOWED,
                    "只应出现定义口径拒绝，实际出现 " + reason);
        }

        // 计数守恒：返回结论总数 == registry.accepted + registry.rejectedTotal；
        // 且 registry 各计数器与逐条结论严格相等（不重不漏）。
        assertEquals(total, accepted + rejected);
        assertEquals(accepted, registry.acceptedCount());
        long registryDefRejects =
                registry.rejectedCounts().get(RejectReason.DEFINITION_VALUE_OUT_OF_RANGE)
                        + registry.rejectedCounts().get(RejectReason.DEFINITION_LABEL_KEY_NOT_ALLOWED);
        assertEquals(rejected, registryDefRejects);
        assertEquals(total, registry.acceptedCount() + registry.rejectedTotal());
        // 落库点数必须严格等于接受数（拒绝不进库，接受不重复）。
        assertEquals(accepted, store.totalSampleCount());
        log.info("[守恒] accepted(registry)={} == 结论接受={} == 落库点数={}；总判定 {} == accepted+rejected",
                registry.acceptedCount(), accepted, store.totalSampleCount(), total);
    }

    @Test
    void concurrentDefinitionUpdatesProduceUniqueContiguousVersions() throws Exception {
        definitions.register("cpu", new MetricDefinition.Spec("%", "v1",
                AggregateFunction.AVG, null, null, Set.of()));
        int updaters = 8;
        int each = 50;
        ExecutorService pool = Executors.newFixedThreadPool(updaters);
        CyclicBarrier barrier = new CyclicBarrier(updaters);
        ConcurrentLinkedQueue<MetricDefinition> created = new ConcurrentLinkedQueue<>();
        for (int u = 0; u < updaters; u++) {
            pool.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (int i = 0; i < each; i++) {
                    created.add(definitions.update("cpu", new MetricDefinition.Spec("%",
                            "updating", AggregateFunction.AVG, null, null, Set.of())));
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        List<Integer> versions = created.stream().map(d -> d.version().version()).sorted().toList();
        log.info("[并发更新] 期望版本 2..{}，实际去重后版本数={}", updaters * each + 1, versions.size());
        assertEquals(updaters * each, created.size());
        assertEquals(updaters * each, versions.stream().distinct().count(), "版本号不得重复");
        for (int v = 2; v <= updaters * each + 1; v++) {
            assertTrue(versions.contains(v), "版本号必须连续，缺失 " + v);
        }
        assertEquals(updaters * each + 1, defStore.versionCount("cpu"));
        assertEquals(updaters * each, registry.definitionsUpdatedCount());
        assertEquals(0L, registry.definitionsRejectedCount());
        log.info("[守恒] 更新成功计数={}，存储版本数={}（含 v1），拒绝计数=0",
                registry.definitionsUpdatedCount(), defStore.versionCount("cpu"));
    }

    @Test
    void concurrentQueriesAlwaysSeeOneSelfConsistentVersion() throws Exception {
        definitions.register("cpu", new MetricDefinition.Spec("%", "v1",
                AggregateFunction.AVG, 0.0, 100.0, Set.of("host")));
        for (int i = 0; i < 200; i++) {
            ingestion.submit("cpu", Map.of("host", "h"), 50.0, T0 + i).join();
        }
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CyclicBarrier barrier = new CyclicBarrier(3);
        AtomicInteger checked = new AtomicInteger();
        // 查询线程 1：反复按最新口径查，结果要么是 v1(AVG) 要么是 v2(MAX)，绝不半成品。
        pool.submit(() -> {
            await(barrier);
            com.github.xingjianluo.metricsalertenginejava.engine.QueryService q =
                    new com.github.xingjianluo.metricsalertenginejava.engine.QueryService(store, defStore);
            for (int i = 0; i < 500; i++) {
                var r = q.queryWithDefinition("cpu", Map.of(), T0, T0 + 1000, 1000, null, null);
                int v = r.definitionVersion().version();
                assertTrue(v == 1 || v == 2);
                // function 必须与版本一致：v1 AVG / v2 MAX，杜绝错配。
                AggregateFunction expected = v == 1 ? AggregateFunction.AVG : AggregateFunction.MAX;
                assertEquals(expected, r.function());
                checked.incrementAndGet();
            }
        });
        // 查询线程 2：钉死 v1，永远看到 AVG 与 v1 版本号。
        pool.submit(() -> {
            await(barrier);
            com.github.xingjianluo.metricsalertenginejava.engine.QueryService q =
                    new com.github.xingjianluo.metricsalertenginejava.engine.QueryService(store, defStore);
            for (int i = 0; i < 500; i++) {
                var r = q.queryWithDefinition("cpu", Map.of(), T0, T0 + 1000, 1000,
                        null, new MetricVersion("cpu", 1));
                assertEquals(1, r.definitionVersion().version());
                assertEquals(AggregateFunction.AVG, r.function());
                checked.incrementAndGet();
            }
        });
        // 更新线程：晚一拍把默认口径改成 MAX。
        pool.submit(() -> {
            await(barrier);
            definitions.update("cpu", new MetricDefinition.Spec("%", "v2",
                    AggregateFunction.MAX, 0.0, 100.0, Set.of("host")));
            checked.incrementAndGet();
        });
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        log.info("[查询自洽] 交错期间完成 {} 次查询：最新查询版本∈{{1,2}} 且 function 与版本匹配；钉死 v1 恒为 AVG",
                checked.get());
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
