package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.IngestionService;
import com.github.xingjianluo.metricsalertenginejava.engine.MetricDefinitionService;
import com.github.xingjianluo.metricsalertenginejava.engine.QueryService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricQueryResult;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;
import com.github.xingjianluo.metricsalertenginejava.model.SeriesAggregation;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 按指标定义版本解读窗口聚合：同一批数据在旧版/新版口径下结论可复现，结果标明所用版本。 */
class DefinitionVersionQueryTest {

    private static final Logger log = LoggerFactory.getLogger(DefinitionVersionQueryTest.class);
    private static final long T0 = 10_000L;
    private static final long W = 1000L;

    private ManualClock clock;
    private TimeSeriesStore tsStore;
    private MetricDefinitionStore defStore;
    private MetricDefinitionService definitions;
    private QueryService query;
    private IngestionService ingestion;

    @BeforeEach
    void setUp() {
        clock = new ManualClock(T0 + 100_000);
        EngineProperties props = TestSupport.properties();
        tsStore = new TimeSeriesStore(props, clock);
        defStore = new MetricDefinitionStore();
        MetricsRegistry registry = new MetricsRegistry();
        definitions = new MetricDefinitionService(defStore, registry, props, clock);
        ingestion = new IngestionService(tsStore, registry, props, clock, definitions);
        ingestion.start();
        query = new QueryService(tsStore, defStore);
    }

    @AfterEach
    void tearDown() {
        ingestion.close();
    }

    /** 经正式服务路径建档：v1 用 register，v2 及以后用 update。 */
    private MetricDefinition makeDef(int version, AggregateFunction fn, Double min, Double max) {
        return makeDef(version, fn, min, max, Set.of("host", "az"));
    }

    private MetricDefinition makeDef(int version, AggregateFunction fn, Double min, Double max,
                                     Set<String> allowedLabelKeys) {
        MetricDefinition.Spec spec = new MetricDefinition.Spec("%", "def v" + version,
                fn, min, max, allowedLabelKeys);
        MetricDefinition stored;
        if (version == 1) {
            stored = definitions.register("cpu", spec);
        } else {
            stored = definitions.update("cpu", spec);
        }
        assertEquals(version, stored.version().version());
        return stored;
    }

    private void put(String metric, Map<String, String> labels, double v, long ts) {
        WriteResult r = ingestion.submit(metric, Labels.of(labels), v, ts).join();
        if (!r.accepted()) {
            throw new IllegalStateException(r.reason() + " " + r.detail());
        }
    }

    @Test
    void sameDataDifferentDefinitionVersionsYieldDifferentConclusions() {
        // v1：默认按 AVG 解读（量程 0..100）；v2：口径改为 MAX（量程放宽到 0..200）。
        MetricDefinition v1 = makeDef(1, AggregateFunction.AVG, 0.0, 100.0);
        MetricDefinition v2 = makeDef(2, AggregateFunction.MAX, 0.0, 200.0);

        // 同一批数据：窗口 [10000,11000) 内两个点 60、90 -> AVG=75, MAX=90。
        put("cpu", Map.of("host", "a"), 60.0, T0 + 100);
        put("cpu", Map.of("host", "a"), 90.0, T0 + 200);
        log.info("[输入] 同一批落库数据 60.0,90.0 于窗口 [10000,11000)；v1 默认口径 AVG，v2 默认口径 MAX");

        MetricQueryResult underV1 = query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, new MetricVersion("cpu", 1));
        MetricQueryResult underV2 = query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, new MetricVersion("cpu", 2));
        MetricQueryResult latest = query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, null);

        double v1Val = firstValue(underV1);
        double v2Val = firstValue(underV2);
        assertEquals(75.0, v1Val, 1e-9);
        assertEquals(90.0, v2Val, 1e-9);
        assertEquals(90.0, firstValue(latest), 1e-9);
        log.info("[判定] v1 结论 value={}（AVG）版本={}；v2 结论 value={}（MAX）版本={}；不指定版本->{} value={}",
                v1Val, underV1.definitionVersion(), v2Val, underV2.definitionVersion(),
                latest.definitionVersion(), firstValue(latest));

        // 结果必须看得出用的是哪个版本。
        assertEquals(new MetricVersion("cpu", 1), underV1.definitionVersion());
        assertEquals(new MetricVersion("cpu", 2), underV2.definitionVersion());
        assertEquals(new MetricVersion("cpu", 2), latest.definitionVersion());
        assertTrue(underV1.definitionRegistered() && underV2.definitionRegistered());
        assertEquals(AggregateFunction.AVG, underV1.function());
        assertEquals(AggregateFunction.MAX, underV2.function());
        assertEquals(AggregateFunction.MAX, latest.function());

        // 复现稳定性：v1 再查仍是 75，且数据只有一份（2 个点）。
        double replayV1 = firstValue(query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, new MetricVersion("cpu", 1)));
        assertEquals(75.0, replayV1, 1e-9);
        assertEquals(2, tsStore.totalSampleCount());
        log.info("[判定] 同数据重复按 v1 查询仍为 75（结论与当时一致），落库点数={}（一份数据两种解读）",
                tsStore.totalSampleCount());
        assertEquals(v1, defStore.get(new MetricVersion("cpu", 1)));
        // v2 与 v1 是不同对象、不同口径。
        assertFalse(v1.equals(v2));
    }

    @Test
    void explicitFunctionOverridesDefaultButVersionStillReported() {
        makeDef(1, AggregateFunction.AVG, 0.0, 100.0);
        put("cpu", Map.of("host", "a"), 60.0, T0 + 100);
        put("cpu", Map.of("host", "a"), 90.0, T0 + 200);
        MetricQueryResult result = query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, AggregateFunction.MIN, new MetricVersion("cpu", 1));
        assertEquals(60.0, firstValue(result), 1e-9);
        assertEquals(AggregateFunction.MIN, result.function());
        assertEquals(new MetricVersion("cpu", 1), result.definitionVersion());
        log.info("[判定] 显式 function=MIN 覆盖定义默认 AVG -> value=60.0，结果仍标明定义版本 {}",
                result.definitionVersion());
    }

    @Test
    void unregisteredMetricRequiresExplicitFunctionAndReportsNoVersion() {
        put("free", Map.of("k", "v"), 42.0, T0 + 100);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> query.queryWithDefinition("free", Map.of(), T0, T0 + W, W, null, null));
        log.info("[判定] 未登记指标不指定算子 -> 报错依据={}", ex.getMessage());
        MetricQueryResult result = query.queryWithDefinition("free", Map.of(),
                T0, T0 + W, W, AggregateFunction.SUM, null);
        assertFalse(result.definitionRegistered());
        assertNull(result.definitionVersion());
        assertEquals(42.0, firstValue(result), 1e-9);
        log.info("[判定] 未登记指标显式 SUM -> value=42.0 definitionVersion=null registered={}",
                result.definitionRegistered());
    }

    @Test
    void relaxedSpecStillReproducesHistoricalConclusionByOldVersion() {
        // v1 窄口径：范围 [0,100]，白名单 {host}；v2 放宽：范围 [0,200]，白名单 {host,rack}。
        // 两版默认聚合都为 AVG， isolate 出“取值范围 + 标签键白名单”的过滤效果。
        makeDef(1, AggregateFunction.AVG, 0.0, 100.0, Set.of("host"));
        put("cpu", Map.of("host", "a"), 60.0, T0 + 100);
        put("cpu", Map.of("host", "a"), 90.0, T0 + 200);
        makeDef(2, AggregateFunction.AVG, 0.0, 200.0, Set.of("host", "rack"));
        // 放宽后才写得进来的数据：越界点（150 超出 v1 上界）与多出来的标签键（rack）。
        put("cpu", Map.of("host", "a"), 150.0, T0 + 300);
        put("cpu", Map.of("host", "b", "rack", "r1"), 70.0, T0 + 400);
        assertEquals(4, tsStore.totalSampleCount());
        log.info("[输入] 落库 4 点：host=a {{60,90}}（v1 时期）+ host=a {{150}}（v1 越界）"
                + " + {{host=b,rack=r1}} 70（v1 不允许的标签键）；v1 口径 [0,100]/{{host}}，v2 [0,200]/{{host,rack}}");

        // 按 v1 回看：越界点 150 不参与聚合，rack 时间线整体不进结果 -> 与当时结论一致 AVG(60,90)=75。
        MetricQueryResult underV1 = query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, new MetricVersion("cpu", 1));
        assertEquals(1, underV1.series().size(), "v1 白名单外的 rack 时间线不得进入结果");
        SeriesAggregation.WindowValue w1 = onlyWindow(underV1, Map.of("host", "a"));
        assertEquals(75.0, w1.value(), 1e-9);
        assertEquals(2, w1.sampleCount(), "v1 越界点 150 不得计入");
        log.info("[判定] 按 v1 复现：series={} value={} sampleCount={}（越界点与 rack 时间线均被排除）",
                underV1.series().get(0).labels(), w1.value(), w1.sampleCount());

        // 按 v2 / 最新：全部 4 点、2 条时间线都进来。
        MetricQueryResult underV2 = query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, new MetricVersion("cpu", 2));
        MetricQueryResult latest = query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, null);
        assertEquals(2, underV2.series().size());
        assertEquals(2, latest.series().size());
        SeriesAggregation.WindowValue w2a = onlyWindow(underV2, Map.of("host", "a"));
        assertEquals(100.0, w2a.value(), 1e-9, "v2 下 AVG(60,90,150)=100");
        assertEquals(3, w2a.sampleCount());
        assertEquals(70.0, onlyWindow(underV2, Map.of("host", "b", "rack", "r1")).value(), 1e-9);
        assertEquals(new MetricVersion("cpu", 2), latest.definitionVersion());
        log.info("[判定] 按 v2/最新：host=a value={} sampleCount={}，rack 时间线 value={}；最新版本={}",
                w2a.value(), w2a.sampleCount(),
                onlyWindow(latest, Map.of("host", "b", "rack", "r1")).value(),
                latest.definitionVersion());

        // 复现稳定性：再按 v1 查一次，结论不变。
        SeriesAggregation.WindowValue replay = onlyWindow(
                query.queryWithDefinition("cpu", Map.of(), T0, T0 + W, W, null,
                        new MetricVersion("cpu", 1)),
                Map.of("host", "a"));
        assertEquals(75.0, replay.value(), 1e-9);
        assertEquals(2, replay.sampleCount());
        log.info("[判定] 重复按 v1 查询仍为 75/2 点：历史结论可稳定复现");
    }

    @Test
    void perVersionQueryCountsMatchWhatEachVersionWouldHaveAccepted() {
        // 计数守恒（查询侧）：按某版本查询得到的点数 == 落库数据中该版本会接受的点数。
        makeDef(1, AggregateFunction.SUM, 0.0, 100.0, Set.of("host"));
        put("cpu", Map.of("host", "a"), 10.0, T0 + 100);
        put("cpu", Map.of("host", "a"), 20.0, T0 + 200);
        makeDef(2, AggregateFunction.SUM, 0.0, 200.0, Set.of("host", "rack"));
        put("cpu", Map.of("host", "a"), 150.0, T0 + 300);
        put("cpu", Map.of("host", "b", "rack", "r1"), 5.0, T0 + 400);
        put("cpu", Map.of("host", "b", "rack", "r1"), 7.0, T0 + 500);
        long stored = tsStore.totalSampleCount();
        assertEquals(5, stored);

        long v1Count = totalSampleCount(query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, new MetricVersion("cpu", 1)));
        long v2Count = totalSampleCount(query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, new MetricVersion("cpu", 2)));
        // v1 只接受 host=a 的 10、20 两点；v2 接受全部 5 点（与落库数严格相等）。
        assertEquals(2, v1Count);
        assertEquals(stored, v2Count);
        log.info("[守恒] 落库={}；按 v1 聚合点数={}（v1 会接受的点数），按 v2 聚合点数={} == 落库数",
                stored, v1Count, v2Count);
    }

    @Test
    void unregisteredMetricStaysLenientForWriteAndQuery() {
        // 未登记指标：任意值（远超任何量程）与任意标签键都能写、能查，行为与接入定义能力前一致。
        WriteResult r1 = ingestion.submit("free", Labels.of(Map.of("anything", "x", "extra", "y")),
                999_999.0, T0 + 100).join();
        WriteResult r2 = ingestion.submit("free", Labels.of(Map.of("anything", "x", "extra", "y")),
                -5.0, T0 + 200).join();
        assertTrue(r1.accepted() && r2.accepted());
        assertNull(r1.definitionVersion(), "未登记指标的写入结论不带定义版本");
        MetricQueryResult result = query.queryWithDefinition("free", Map.of(),
                T0, T0 + W, W, AggregateFunction.SUM, null);
        assertFalse(result.definitionRegistered());
        assertNull(result.definitionVersion());
        assertEquals(999_994.0, firstValue(result), 1e-9);
        assertEquals(2, result.series().get(0).windows().stream()
                .filter(w -> !w.missing()).findFirst().orElseThrow().sampleCount());
        log.info("[判定] 未登记指标宽松写查：值 999999/-5、标签键 anything/extra 均接受，"
                + "显式 SUM -> {}，definitionVersion=null", firstValue(result));
    }

    @Test
    void oldVersionDefinitionsRemainRetrievable() {
        MetricDefinition v1 = makeDef(1, AggregateFunction.AVG, 0.0, 100.0);
        MetricDefinition v2 = makeDef(2, AggregateFunction.MAX, 0.0, 200.0);
        MetricDefinition v3 = makeDef(3, AggregateFunction.MIN, null, null);
        // 每一版都能按版本号原样取回（不可变记录，equals 逐字段比较）。
        assertEquals(v1, definitions.get(new MetricVersion("cpu", 1)));
        assertEquals(v2, definitions.get(new MetricVersion("cpu", 2)));
        assertEquals(v3, definitions.get(new MetricVersion("cpu", 3)));
        assertEquals(java.util.List.of(v1, v2, v3), definitions.versions("cpu"));
        assertEquals(v3, definitions.latest("cpu"));
        log.info("[判定] v1/v2/v3 均可按版本取回且内容不变；versions() 按序返回 3 版；latest=v3");
    }

    @Test
    void unknownOrMismatchedVersionFailsLoudly() {
        makeDef(1, AggregateFunction.AVG, 0.0, 100.0);
        // 版本不存在：明确报错，不能静默按别的口径给结论。
        IllegalArgumentException notFound = assertThrows(IllegalArgumentException.class,
                () -> query.queryWithDefinition("cpu", Map.of(), T0, T0 + W, W, null,
                        new MetricVersion("cpu", 99)));
        assertTrue(notFound.getMessage().contains("not found"));
        log.info("[判定] 查询不存在的版本 cpu#def-v99 -> 报错依据={}", notFound.getMessage());
        // 版本与指标对不上：同样明确报错。
        IllegalArgumentException mismatch = assertThrows(IllegalArgumentException.class,
                () -> query.queryWithDefinition("cpu", Map.of(), T0, T0 + W, W, null,
                        new MetricVersion("mem", 1)));
        assertTrue(mismatch.getMessage().contains("does not belong to metric"));
        log.info("[判定] 版本 mem#def-v1 用于查询 cpu -> 报错依据={}", mismatch.getMessage());
    }

    private static SeriesAggregation.WindowValue onlyWindow(MetricQueryResult result,
                                                            Map<String, String> labels) {
        Labels expected = Labels.of(labels);
        SeriesAggregation series = result.series().stream()
                .filter(s -> s.labels().equals(expected))
                .findFirst()
                .orElseThrow(() -> new AssertionError("series not found: " + labels));
        return series.windows().stream().filter(w -> !w.missing()).findFirst().orElseThrow();
    }

    private static long totalSampleCount(MetricQueryResult result) {
        return result.series().stream()
                .flatMap(s -> s.windows().stream())
                .mapToLong(SeriesAggregation.WindowValue::sampleCount)
                .sum();
    }

    private static double firstValue(MetricQueryResult result) {
        SeriesAggregation series = result.series().get(0);
        return series.windows().stream().filter(w -> !w.missing()).findFirst().orElseThrow().value();
    }
}
