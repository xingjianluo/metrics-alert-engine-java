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

    @Test
    void widenedRangeAndLabelKeysDoNotLeakIntoOldVersionReplay() {
        // v1：默认 AVG，量程 [0,100]，只允许 host 标签键。
        MetricDefinition v1 = definitions.register("cpu", new MetricDefinition.Spec("%", "v1 窄口径",
                AggregateFunction.AVG, 0.0, 100.0, Set.of("host")));
        // v1 时期写入：两个合法点 60、90（窗口内 AVG=75 是“当时的结论”）。
        put("cpu", Map.of("host", "a"), 60.0, T0 + 100);
        put("cpu", Map.of("host", "a"), 90.0, T0 + 200);
        log.info("[输入] v1 口径=AVG 范围=[0,100] 白名单=[host]；v1 时期落库 60.0、90.0 -> 当时结论 AVG=75");

        // 口径放宽到 v2：量程放宽到 [0,200]，白名单放宽到 [host, rack]。
        MetricDefinition v2 = definitions.update("cpu", new MetricDefinition.Spec("%", "v2 放宽",
                AggregateFunction.AVG, 0.0, 200.0, Set.of("host", "rack")));
        // v2 时期写入两类“v1 不会接受”的数据：
        //  (a) 越界点：150 超出 v1 量程（v1 会以 DEFINITION_VALUE_OUT_OF_RANGE 拒绝）；
        //  (b) 多出标签键的时间线：rack 不在 v1 白名单（v1 会以 DEFINITION_LABEL_KEY_NOT_ALLOWED 拒绝）。
        put("cpu", Map.of("host", "a"), 150.0, T0 + 300);
        put("cpu", Map.of("host", "a", "rack", "r1"), 80.0, T0 + 150);
        log.info("[输入] v2 放宽为 范围=[0,200] 白名单=[host,rack]；v2 时期又落库越界点 150.0 "
                + "与带 rack 键的时间线 80.0（两者 v1 当时都会拒绝）");

        // 按 v1 复现：越界点 150 与 rack 时间线都不能进入结果，结论必须仍是当时的 75。
        MetricQueryResult underV1 = query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, new MetricVersion("cpu", 1));
        assertEquals(new MetricVersion("cpu", 1), underV1.definitionVersion());
        assertEquals(1, underV1.series().size(), "v1 口径下 rack 时间线必须整条排除");
        SeriesAggregation s1 = underV1.series().get(0);
        assertEquals(Map.of("host", "a"), s1.labels().asMap());
        SeriesAggregation.WindowValue w1 = s1.windows().get(0);
        assertEquals(2, w1.sampleCount(), "v1 口径下越界点 150 不得计入");
        assertEquals(75.0, w1.value(), 1e-9);
        log.info("[判定] 按 v1 复现：series={} 窗口值={} 点数={}（越界点与 rack 时间线均被 v1 口径排除，"
                + "结论与当时一致）", underV1.series().size(), w1.value(), w1.sampleCount());

        // 按 v2（最新）：放宽后的数据全部可见，同一条 host=a 时间线 AVG=(60+90+150)/3=100。
        MetricQueryResult underV2 = query.queryWithDefinition("cpu", Map.of(),
                T0, T0 + W, W, null, null);
        assertEquals(new MetricVersion("cpu", 2), underV2.definitionVersion());
        assertEquals(2, underV2.series().size(), "v2 口径下 rack 时间线应可见");
        SeriesAggregation hostOnly = underV2.series().stream()
                .filter(s -> s.labels().asMap().equals(Map.of("host", "a"))).findFirst().orElseThrow();
        assertEquals(3, hostOnly.windows().get(0).sampleCount());
        assertEquals(100.0, hostOnly.windows().get(0).value(), 1e-9);
        log.info("[判定] 按 v2（最新）：series={} host=a 窗口值={} 点数={}（放宽后数据全部计入）",
                underV2.series().size(), hostOnly.windows().get(0).value(),
                hostOnly.windows().get(0).sampleCount());

        // 计数守恒：4 个点全部落库（接受数==落库点数），v1 可见 2 点 + v1 排除 2 点 == 4。
        assertEquals(4, tsStore.totalSampleCount());
        log.info("[守恒] 落库点数=4 == 接受数；v1 可见 2 点 + v1 口径排除 2 点（越界 1 + rack 线 1）== 4，不重不漏");

        // 旧版本定义仍能按版本号取回，且与 v2 是不同口径。
        assertEquals(v1, definitions.get(new MetricVersion("cpu", 1)));
        assertEquals(v2, definitions.get(new MetricVersion("cpu", 2)));
        assertEquals(2, definitions.versions("cpu").size());
        log.info("[判定] 旧版本定义可取回：v1={} v2={}，版本列表={}",
                v1.version(), v2.version(), definitions.versions("cpu").stream()
                        .map(d -> d.version().version()).toList());
    }

    @Test
    void unknownVersionAndMismatchedMetricFailLoudly() {
        makeDef(1, AggregateFunction.AVG, 0.0, 100.0);
        put("cpu", Map.of("host", "a"), 60.0, T0 + 100);

        // 指定了不存在的历史版本：必须明确报错，不能静默按别的口径给结论。
        IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
                () -> query.queryWithDefinition("cpu", Map.of(),
                        T0, T0 + W, W, null, new MetricVersion("cpu", 99)));
        log.info("[判定] 不存在的版本 cpu#def-v99 -> 报错依据={}", unknown.getMessage());

        // 版本与指标对不上：同样明确报错。
        IllegalArgumentException mismatched = assertThrows(IllegalArgumentException.class,
                () -> query.queryWithDefinition("cpu", Map.of(),
                        T0, T0 + W, W, null, new MetricVersion("other", 1)));
        log.info("[判定] 版本属于 other 却查 cpu -> 报错依据={}", mismatched.getMessage());

        // 从未登记的指标指定版本：也是“版本不存在”，明确报错。
        IllegalArgumentException ghost = assertThrows(IllegalArgumentException.class,
                () -> query.queryWithDefinition("ghost", Map.of(),
                        T0, T0 + W, W, null, new MetricVersion("ghost", 1)));
        log.info("[判定] 未登记指标 ghost 指定版本 -> 报错依据={}", ghost.getMessage());
    }

    @Test
    void unregisteredMetricStaysLenientForAnyValueAndLabelKeys() {
        // 未登记指标：任意有限值、任意格式合法的标签键都能写入（宽松行为不收紧）。
        put("free", Map.of("any_key", "x", "another", "y"), 1_000_000.0, T0 + 100);
        put("free", Map.of("any_key", "x", "another", "y"), -5.0, T0 + 200);
        log.info("[输入] 未登记指标 free 写入 1000000.0、-5.0，标签键 any_key/another 无任何白名单约束");

        MetricQueryResult result = query.queryWithDefinition("free", Map.of(),
                T0, T0 + W, W, AggregateFunction.AVG, null);
        assertFalse(result.definitionRegistered());
        assertNull(result.definitionVersion());
        assertEquals(1, result.series().size());
        assertEquals(2, result.series().get(0).windows().get(0).sampleCount());
        assertEquals(499_997.5, result.series().get(0).windows().get(0).value(), 1e-6);
        log.info("[判定] 未登记指标显式 AVG -> value={} 点数={} definitionVersion=null（宽松可查）",
                result.series().get(0).windows().get(0).value(),
                result.series().get(0).windows().get(0).sampleCount());
    }

    private static double firstValue(MetricQueryResult result) {
        SeriesAggregation series = result.series().get(0);
        return series.windows().stream().filter(w -> !w.missing()).findFirst().orElseThrow().value();
    }
}
