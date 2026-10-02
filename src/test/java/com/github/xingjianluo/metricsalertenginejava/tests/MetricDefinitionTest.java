package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.IngestionService;
import com.github.xingjianluo.metricsalertenginejava.engine.QueryService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricQueryResult;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore.DefinitionSpec;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 指标定义：版本化登记、按版本复现查询、写入校验、容量上限、并发守恒。 */
class MetricDefinitionTest {

    private static final Logger log = LoggerFactory.getLogger(MetricDefinitionTest.class);
    private static final long T0 = 1_000_000L;

    private static DefinitionSpec spec(AggregateFunction fn, Double min, Double max,
                                       Set<String> allowedKeys) {
        return new DefinitionSpec("unit", "desc", fn, min, max, allowedKeys);
    }

    @Test
    void registerAndUpdateProduceImmutableRetrievableVersions() {
        ManualClock clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        MetricDefinitionStore defs = new MetricDefinitionStore(props, clock, new MetricsRegistry());

        MetricDefinition v1 = defs.register("cpu",
                spec(AggregateFunction.AVG, 0.0, 100.0, Set.of("host")));
        log.info("[场景] 登记定义 cpu v1：默认口径={} 值域=[{},{}] 允许标签键={}",
                v1.defaultFunction(), v1.minValue(), v1.maxValue(), v1.allowedLabelKeys());
        assertEquals(1, v1.version());

        MetricDefinition v2 = defs.update("cpu",
                spec(AggregateFunction.MAX, 0.0, 1000.0, Set.of("host", "region")));
        log.info("[场景] 更新定义 cpu v2：默认口径={} 值域=[{},{}] 允许标签键={}",
                v2.defaultFunction(), v2.minValue(), v2.maxValue(), v2.allowedLabelKeys());
        assertEquals(2, v2.version());

        MetricDefinition fetchedV1 = defs.get("cpu", 1);
        log.info("[判定] 旧版本可取回且未被覆盖：v1 值域上界={}（登记时=100），latest 版本={}",
                fetchedV1.maxValue(), defs.latest("cpu").orElseThrow().version());
        assertEquals(100.0, fetchedV1.maxValue(), "旧版本定义必须保持登记时的口径");
        assertEquals(AggregateFunction.AVG, fetchedV1.defaultFunction());
        assertEquals(2, defs.latest("cpu").orElseThrow().version());
        assertEquals(2, defs.versions("cpu").size());
        assertThrows(IllegalStateException.class,
                () -> defs.register("cpu", spec(AggregateFunction.SUM, null, null, Set.of())),
                "重复登记同一指标必须被拒绝");
        assertThrows(IllegalArgumentException.class, () -> defs.get("cpu", 99),
                "不存在的版本必须明确报错");
    }

    @Test
    void queryByVersionReproducesHistoricalConclusions() {
        ManualClock clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        MetricsRegistry registry = new MetricsRegistry();
        TimeSeriesStore store = new TimeSeriesStore(props, clock);
        MetricDefinitionStore defs = new MetricDefinitionStore(props, clock, registry);
        QueryService query = new QueryService(store, defs);
        try (IngestionService ingestion = new IngestionService(store, registry, props, clock, defs)) {
            ingestion.start();

            defs.register("cpu", spec(AggregateFunction.AVG, 0.0, 100.0, Set.of("host")));
            // v1 口径下落库两条：90、100（窗口 [T0,T0+1000)）。
            assertTrue(ingestion.submitAndWait("cpu", Labels.of(Map.of("host", "a")), 90, T0 + 100).accepted());
            assertTrue(ingestion.submitAndWait("cpu", Labels.of(Map.of("host", "a")), 100, T0 + 200).accepted());
            MetricQueryResult atThatTime = query.queryWithDefinition("cpu", null, T0, T0 + 2000,
                    1000, null, null);
            double historicalValue = atThatTime.series().get(0).windows().get(0).value();
            log.info("[场景] v1 口径(AVG,[0,100],{{host}})下当时结论：窗口0 值={} 使用版本={} 算子={}",
                    historicalValue, atThatTime.definitionVersion(), atThatTime.function());
            assertEquals(95.0, historicalValue);
            assertEquals(1, atThatTime.definitionVersion());

            // 口径变更：默认算子改 MAX、值域放宽到 [0,1000]、允许新标签键 region。
            defs.update("cpu", spec(AggregateFunction.MAX, 0.0, 1000.0, Set.of("host", "region")));
            // 新口径下才能写入的数据：500 超出 v1 值域；region 是 v1 不允许的标签键。
            assertTrue(ingestion.submitAndWait("cpu", Labels.of(Map.of("host", "a")), 500, T0 + 300).accepted());
            assertTrue(ingestion.submitAndWait("cpu", Labels.of(Map.of("host", "a", "region", "cn")),
                    50, T0 + 400).accepted());

            MetricQueryResult oldView = query.queryWithDefinition("cpu", null, T0, T0 + 2000,
                    1000, null, 1);
            double reproduced = oldView.series().get(0).windows().get(0).value();
            log.info("[判定] 同一批落库数据按 v1 复现：窗口0 值={}（当时={}），series 数={}（v1 只允许 host 键）",
                    reproduced, historicalValue, oldView.series().size());
            assertEquals(historicalValue, reproduced, "旧版本定义下必须复现当时结论");
            assertEquals(1, oldView.series().size(), "带 region 键的时间线在 v1 口径下不参与解读");
            assertEquals(1, oldView.definitionVersion());
            assertEquals(AggregateFunction.AVG, oldView.function(), "未指定算子时取该版本默认口径");

            MetricQueryResult newView = query.queryWithDefinition("cpu", null, T0, T0 + 2000,
                    1000, null, null);
            // 按标签规范串排序：{host="a",region="cn"} 在 {host="a"} 之前（',' < '}'），按标签内容定位。
            double newValue = newView.series().stream()
                    .filter(s -> s.labels().get("region") == null)
                    .findFirst().orElseThrow().windows().get(0).value();
            log.info("[判定] 不指定版本走最新 v2：窗口0 值={}（MAX 口径含 500），series 数={} 版本={}",
                    newValue, newView.series().size(), newView.definitionVersion());
            assertEquals(2, newView.definitionVersion(), "不指定版本时默认走最新版本");
            assertEquals(AggregateFunction.MAX, newView.function());
            assertEquals(500.0, newValue, "新版本口径要反映新值域与新算子");
            assertEquals(2, newView.series().size(), "v2 允许 region 键，两条时间线都参与解读");
        }
    }

    @Test
    void unregisteredMetricKeepsLenientWriteAndQuery() {
        ManualClock clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        MetricsRegistry registry = new MetricsRegistry();
        TimeSeriesStore store = new TimeSeriesStore(props, clock);
        MetricDefinitionStore defs = new MetricDefinitionStore(props, clock, registry);
        QueryService query = new QueryService(store, defs);
        try (IngestionService ingestion = new IngestionService(store, registry, props, clock, defs)) {
            ingestion.start();
            // 未登记定义的指标：任意值、任意合法标签键都应照常写入（兼容既有调用方）。
            WriteResult r1 = ingestion.submitAndWait("raw", Labels.of(Map.of("any", "x")), 1e9, T0);
            WriteResult r2 = ingestion.submitAndWait("raw", Labels.of(Map.of("k", "v")), -1e9, T0 + 1);
            log.info("[判定] 未登记指标写入：超大值 accepted={} 负值 accepted={}（宽松模式不受影响）",
                    r1.accepted(), r2.accepted());
            assertTrue(r1.accepted());
            assertTrue(r2.accepted());

            MetricQueryResult result = query.queryWithDefinition("raw", null, T0, T0 + 1000,
                    1000, null, null);
            log.info("[判定] 未登记指标查询：版本标记={}（null=宽松）算子={} series 数={}",
                    result.definitionVersion(), result.function(), result.series().size());
            assertEquals(null, result.definitionVersion(), "未登记指标必须显式标记为无定义版本");
            assertEquals(AggregateFunction.SUM, result.function(), "未登记指标缺省算子保持 SUM");
            assertEquals(2, result.series().size());
            assertEquals(0, registry.rejectedTotal(), "宽松模式下不应产生任何拒绝");
        }
    }

    @Test
    void outOfRangeValueAndDisallowedLabelKeyRejectedDistinctly() {
        ManualClock clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        MetricsRegistry registry = new MetricsRegistry();
        TimeSeriesStore store = new TimeSeriesStore(props, clock);
        MetricDefinitionStore defs = new MetricDefinitionStore(props, clock, registry);
        try (IngestionService ingestion = new IngestionService(store, registry, props, clock, defs)) {
            ingestion.start();
            defs.register("temp", spec(AggregateFunction.AVG, 0.0, 50.0, Set.of("room")));

            WriteResult outOfRange = ingestion.submitAndWait("temp",
                    Labels.of(Map.of("room", "a")), 60, T0);
            WriteResult badKey = ingestion.submitAndWait("temp",
                    Labels.of(Map.of("room", "a", "device", "x")), 10, T0 + 1);
            // 数据本身非法（标签名不合法）：与定义校验拒绝是不同的原因。
            WriteResult invalidLabel = ingestion.submit("temp",
                    Map.of("1bad", "v"), 10, T0 + 2).join();
            WriteResult ok = ingestion.submitAndWait("temp",
                    Labels.of(Map.of("room", "a")), 25, T0 + 3);
            log.info("[判定] 值越界 reason={} detail={}", outOfRange.reason(), outOfRange.detail());
            log.info("[判定] 标签键越界 reason={} detail={}", badKey.reason(), badKey.detail());
            log.info("[判定] 数据本身非法 reason={}（与定义拒绝可区分）", invalidLabel.reason());
            assertFalse(outOfRange.accepted());
            assertEquals(RejectReason.VALUE_OUT_OF_RANGE, outOfRange.reason());
            assertFalse(badKey.accepted());
            assertEquals(RejectReason.LABEL_KEY_NOT_ALLOWED, badKey.reason());
            assertFalse(invalidLabel.accepted());
            assertEquals(RejectReason.INVALID_LABEL, invalidLabel.reason());
            assertTrue(ok.accepted());

            Map<RejectReason, Long> rejected = registry.rejectedCounts();
            log.info("[判定] 计数可区分且守恒：VALUE_OUT_OF_RANGE={} LABEL_KEY_NOT_ALLOWED={} "
                            + "INVALID_LABEL={} accepted={}",
                    rejected.get(RejectReason.VALUE_OUT_OF_RANGE),
                    rejected.get(RejectReason.LABEL_KEY_NOT_ALLOWED),
                    rejected.get(RejectReason.INVALID_LABEL), registry.acceptedCount());
            assertEquals(1L, rejected.get(RejectReason.VALUE_OUT_OF_RANGE));
            assertEquals(1L, rejected.get(RejectReason.LABEL_KEY_NOT_ALLOWED));
            assertEquals(1L, rejected.get(RejectReason.INVALID_LABEL),
                    "定义拒绝不能与数据本身非法混淆");
            assertEquals(1, registry.acceptedCount());
            assertEquals(1, store.totalSampleCount(), "只有合法那条落库");
            assertEquals(3, registry.rejectedTotal(), "三种拒绝原因各计一次");
        }
    }

    @Test
    void definitionAndVersionCapsRejectWithObservableCount() {
        ManualClock clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        props.setMaxMetricDefinitions(2);
        props.setMaxDefinitionVersionsPerMetric(2);
        MetricsRegistry registry = new MetricsRegistry();
        MetricDefinitionStore defs = new MetricDefinitionStore(props, clock, registry);

        defs.register("m1", spec(AggregateFunction.SUM, null, null, Set.of()));
        defs.register("m2", spec(AggregateFunction.SUM, null, null, Set.of()));
        MetricDefinitionStore.DefinitionLimitException limitEx = assertThrows(
                MetricDefinitionStore.DefinitionLimitException.class,
                () -> defs.register("m3", spec(AggregateFunction.SUM, null, null, Set.of())));
        log.info("[判定] 定义数量超限被拒绝：{}；definitionLimitRejected={}",
                limitEx.getMessage(), registry.definitionLimitRejectedCount());
        assertEquals(1, registry.definitionLimitRejectedCount(), "超限拒绝必须留下可观测计数");

        defs.update("m1", spec(AggregateFunction.AVG, null, null, Set.of()));
        assertThrows(MetricDefinitionStore.DefinitionLimitException.class,
                () -> defs.update("m1", spec(AggregateFunction.MAX, null, null, Set.of())),
                "版本数量超限必须明确拒绝");
        log.info("[判定] 版本数量超限被拒绝；m1 版本数={}（上限 2），v1 仍可取回：{}",
                defs.versions("m1").size(), defs.get("m1", 1).defaultFunction());
        assertEquals(2, registry.definitionLimitRejectedCount());
        assertEquals(2, defs.versions("m1").size(), "超限后旧版本不受影响");
        assertEquals(AggregateFunction.SUM, defs.get("m1", 1).defaultFunction());
    }

    @Test
    void concurrentDefinitionUpdatesWritesAndQueriesConserveCounts() throws Exception {
        ManualClock clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        props.setIngestQueueCapacity(10_000);
        props.setMaxSeriesPerMetric(100_000);
        props.setMaxTotalSeries(100_000);
        MetricsRegistry registry = new MetricsRegistry();
        TimeSeriesStore store = new TimeSeriesStore(props, clock);
        MetricDefinitionStore defs = new MetricDefinitionStore(props, clock, registry);
        QueryService query = new QueryService(store, defs);

        int threads = 4;
        int perThread = 200;
        AtomicLong submitted = new AtomicLong();
        AtomicLong queryCount = new AtomicLong();
        long[] baseline = new long[2]; // [0]=并发前 accepted，[1]=并发前 rejected
        try (IngestionService ingestion = new IngestionService(store, registry, props, clock, defs)) {
            ingestion.start();
            defs.register("lat", spec(AggregateFunction.AVG, 0.0, 10_000.0, Set.of("thread", "i")));

            // 确定性前置：值域收窄时越界值必被拒（证明交错下校验按当时版本生效）。
            defs.update("lat", spec(AggregateFunction.AVG, 0.0, 5_000.0, Set.of("thread", "i")));
            WriteResult rejected = ingestion.submitAndWait("lat",
                    Labels.of(Map.of("thread", "x", "i", "0")), 8_000, T0 - 1);
            log.info("[场景] 值域收窄到 [0,5000] 后写 8000：reason={}", rejected.reason());
            assertEquals(RejectReason.VALUE_OUT_OF_RANGE, rejected.reason());
            defs.update("lat", spec(AggregateFunction.AVG, 0.0, 10_000.0, Set.of("thread", "i")));

            // 并发阶段基线：前置阶段的接受/拒绝不计入守恒断言。
            baseline[0] = registry.acceptedCount();
            baseline[1] = registry.rejectedTotal();

            ExecutorService pool = Executors.newFixedThreadPool(threads + 2);
            CountDownLatch done = new CountDownLatch(threads + 2);
            // 写线程：值落在 [0,10000)，任一版本口径下都合法。
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < perThread; i++) {
                            Labels labels = Labels.of(Map.of("thread", String.valueOf(tid),
                                    "i", String.valueOf(i)));
                            ingestion.submitAndWait("lat", labels, (tid * perThread + i) % 10_000,
                                    T0 + i);
                            submitted.incrementAndGet();
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            // 定义更新线程：在两种值域口径间来回切换（版本号持续递增）。
            pool.submit(() -> {
                try {
                    for (int u = 0; u < 20; u++) {
                        double max = u % 2 == 0 ? 10_000.0 : 9_000.0;
                        defs.update("lat", spec(AggregateFunction.AVG, 0.0, max,
                                Set.of("thread", "i")));
                    }
                } finally {
                    done.countDown();
                }
            });
            // 查询线程：与写和定义更新交错，每次都应钉住某个版本、结论自洽。
            pool.submit(() -> {
                try {
                    for (int q = 0; q < 50; q++) {
                        MetricQueryResult r = query.queryWithDefinition("lat", null,
                                T0, T0 + perThread, 100, null, null);
                        assertNotNull(r.definitionVersion(), "已登记指标查询必须带出版本号");
                        assertTrue(r.definitionVersion() >= 1);
                        assertEquals(AggregateFunction.AVG, r.function());
                        queryCount.incrementAndGet();
                    }
                } finally {
                    done.countDown();
                }
            });
            assertTrue(done.await(60, TimeUnit.SECONDS));
            pool.shutdown();
        }

        long accepted = registry.acceptedCount() - baseline[0];
        long rejectedTotal = registry.rejectedTotal() - baseline[1];
        log.info("[判定] 并发交错后计数守恒：submitted={} accepted={} rejected={}（并发阶段） "
                        + "全局值域拒绝={}（含前置确定性 1 次）落库点数={} 定义版本数={} 并发查询次数={}",
                submitted.get(), accepted, rejectedTotal,
                registry.rejectedCounts().get(RejectReason.VALUE_OUT_OF_RANGE),
                store.totalSampleCount(), defs.versions("lat").size(), queryCount.get());
        assertEquals(threads * perThread, submitted.get());
        assertEquals(submitted.get(), accepted + rejectedTotal,
                "每次提交必须恰好落入 接受/拒绝 之一，不丢数不重复计数");
        assertEquals(accepted, store.totalSampleCount(), "接受数必须与落库点数一致");
        assertEquals(1L, registry.rejectedCounts().get(RejectReason.VALUE_OUT_OF_RANGE),
                "并发阶段的写入值都在最窄值域内，只有前置那次越界拒绝");
        // 交错结束后按 v1 复现：v1 口径 [0,10000] 下全部点合法，窗口值确定。
        MetricQueryResult v1View = query.queryWithDefinition("lat", null, T0, T0 + perThread,
                100, null, 1);
        long totalInWindows = v1View.series().stream()
                .flatMap(s -> s.windows().stream()).mapToLong(w -> w.sampleCount()).sum();
        log.info("[判定] 交错后按 v1 复现：版本={} 窗口内总点数={}（= 落库点数 {}）",
                v1View.definitionVersion(), totalInWindows, store.totalSampleCount());
        assertEquals(1, v1View.definitionVersion());
        assertEquals(store.totalSampleCount(), totalInWindows,
                "v1 口径下所有落库点都参与解读，复现结论与落库一致");
    }
}
