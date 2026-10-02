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

    /** 经正式服务路径建档：v1 用 register，v2 用 update。 */
    private MetricDefinition makeDef(int version, AggregateFunction fn, Double min, Double max) {
        MetricDefinition.Spec spec = new MetricDefinition.Spec("%", "def v" + version,
                fn, min, max, Set.of("host", "az"));
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

    private static double firstValue(MetricQueryResult result) {
        SeriesAggregation series = result.series().get(0);
        return series.windows().stream().filter(w -> !w.missing()).findFirst().orElseThrow().value();
    }
}
