package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.IngestionService;
import com.github.xingjianluo.metricsalertenginejava.engine.MetricDefinitionService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
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

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 登记定义后的写入校验：值域 / 标签键白名单拒绝与数据本身拒绝原因可区分；未登记保持宽松。 */
class DefinitionIngestionValidationTest {

    private static final Logger log = LoggerFactory.getLogger(DefinitionIngestionValidationTest.class);
    private static final long T0 = 10_000L;

    private ManualClock clock;
    private TimeSeriesStore store;
    private MetricsRegistry registry;
    private MetricDefinitionService definitions;
    private IngestionService ingestion;

    @BeforeEach
    void setUp() {
        clock = new ManualClock(T0);
        EngineProperties props = TestSupport.properties();
        store = new TimeSeriesStore(props, clock);
        registry = new MetricsRegistry();
        MetricDefinitionStore defStore = new MetricDefinitionStore();
        definitions = new MetricDefinitionService(defStore, registry, props, clock);
        ingestion = new IngestionService(store, registry, props, clock, definitions);
        ingestion.start();
    }

    @AfterEach
    void tearDown() {
        ingestion.close();
    }

    private WriteResult write(String metric, Map<String, String> labels, double value, long ts) {
        return ingestion.submit(metric, Labels.of(labels), value, ts).join();
    }

    @Test
    void unregisteredMetricStillWritesPermissively() {
        // 未登记任何定义：任意值、任意标签键都按旧的宽松方式正常写入。
        WriteResult r1 = write("free_metric", Map.of("any_key", "a", "another", "b"), 123456.789, T0);
        WriteResult r2 = write("free_metric", Map.of(), -Double.MAX_VALUE, T0 + 1);
        log.info("[输入] 未登记指标 free_metric 写入 value={} 标签={}", 123456.789, "{any_key,another}");
        assertTrue(r1.accepted(), r1.detail());
        assertTrue(r2.accepted(), r2.detail());
        assertNull(r1.definitionVersion());
        log.info("[判定] 未登记指标正常接受，definitionVersion=null（宽松模式），accepted={}",
                registry.acceptedCount());
        assertEquals(2L, registry.acceptedCount());
        assertEquals(0L, registry.rejectedTotal());
        assertEquals(2, store.seriesCount("free_metric"));
    }

    @Test
    void valueOutOfRangeRejectedDistinctly() {
        definitions.register("cpu", new MetricDefinition.Spec("%", "0..100",
                AggregateFunction.AVG, 0.0, 100.0, Set.of("host")));
        WriteResult below = write("cpu", Map.of("host", "a"), -0.01, T0);
        WriteResult above = write("cpu", Map.of("host", "a"), 100.01, T0 + 1);
        WriteResult nan = write("cpu", Map.of("host", "a"), Double.NaN, T0 + 2);
        log.info("[输入] cpu 范围 [0,100]：-0.01 / 100.01 / NaN");
        for (WriteResult r : java.util.List.of(below, above, nan)) {
            assertFalse(r.accepted());
            assertEquals(RejectReason.DEFINITION_VALUE_OUT_OF_RANGE, r.reason());
            assertEquals(1, r.definitionVersion().version());
            log.info("[判定] value={} 被拒绝 reason={} 依据={}",
                    r.sample().value(), r.reason(), r.detail());
        }
        // 边界值合法（闭区间）。
        WriteResult lowEdge = write("cpu", Map.of("host", "a"), 0.0, T0 + 3);
        WriteResult highEdge = write("cpu", Map.of("host", "a"), 100.0, T0 + 4);
        assertTrue(lowEdge.accepted() && highEdge.accepted());
        log.info("[判定] 边界值 0.0 与 100.0 均接受（范围为闭区间）");
        assertEquals(2L, registry.acceptedCount());
        assertEquals(3L, registry.rejectedCounts().get(RejectReason.DEFINITION_VALUE_OUT_OF_RANGE));
        // 越界数据绝不能进入存储。
        assertEquals(2, store.totalSampleCount());
    }

    @Test
    void disallowedLabelKeyRejectedDistinctly() {
        definitions.register("cpu", new MetricDefinition.Spec("%", null,
                AggregateFunction.AVG, null, null, Set.of("host")));
        WriteResult bad = write("cpu", Map.of("host", "a", "rack", "r1"), 50.0, T0);
        log.info("[输入] cpu 白名单=[host]，写入标签 {host,rack}");
        assertFalse(bad.accepted());
        assertEquals(RejectReason.DEFINITION_LABEL_KEY_NOT_ALLOWED, bad.reason());
        assertEquals(1, bad.definitionVersion().version());
        log.info("[判定] 被拒绝 reason={} 依据={}", bad.reason(), bad.detail());

        WriteResult good = write("cpu", Map.of("host", "b"), 50.0, T0 + 1);
        assertTrue(good.accepted());
        log.info("[判定] 仅用白名单键 host 时接受；accepted={} rejected={}",
                registry.acceptedCount(), registry.rejectedTotal());
        assertEquals(1L, registry.acceptedCount());
        assertEquals(1L, registry.rejectedCounts().get(RejectReason.DEFINITION_LABEL_KEY_NOT_ALLOWED));
        assertEquals(0L, registry.rejectedCounts().get(RejectReason.INVALID_LABEL));
    }

    @Test
    void emptyWhitelistMeansUnrestrictedEvenAfterRegistration() {
        definitions.register("latency", new MetricDefinition.Spec("ms", null,
                AggregateFunction.AVG, 0.0, null, Set.of()));
        WriteResult r = write("latency", Map.of("whatever", "x", "keys", "y"), 12.0, T0);
        log.info("[输入] latency 已登记但白名单为空集，写入标签 {whatever,keys}");
        assertTrue(r.accepted(), r.detail());
        log.info("[判定] 空白名单=不限制标签键（与未登记语义一致），只按值域校验");
    }

    @Test
    void definitionRejectionsAreSeparatedFromDataRejections() {
        definitions.register("cpu", new MetricDefinition.Spec("%", null,
                AggregateFunction.AVG, 0.0, 100.0, Set.of("host")));
        // 数据本身问题：标签格式非法 -> INVALID_LABEL（入队前，未走到定义校验）。
        WriteResult invalidLabel = ingestion.submit("cpu",
                Map.of("1bad", "v"), 50.0, T0).join();
        // 定义口径问题：标签格式合法但不在白名单。
        WriteResult notAllowed = write("cpu", Map.of("rack", "r1"), 50.0, T0 + 1);
        log.info("[输入] 标签 1bad（格式非法）vs rack（格式合法但不在白名单 [host]）");
        assertEquals(RejectReason.INVALID_LABEL, invalidLabel.reason());
        assertEquals(RejectReason.DEFINITION_LABEL_KEY_NOT_ALLOWED, notAllowed.reason());
        log.info("[判定] 数据问题={} 与 定义问题={} 明确区分",
                invalidLabel.reason(), notAllowed.reason());
        assertEquals(1L, registry.rejectedCounts().get(RejectReason.INVALID_LABEL));
        assertEquals(1L,
                registry.rejectedCounts().get(RejectReason.DEFINITION_LABEL_KEY_NOT_ALLOWED));
    }

    @Test
    void newerDefinitionVersionValidatesSubsequentWrites() {
        definitions.register("cpu", new MetricDefinition.Spec("%", "v1 0..100",
                AggregateFunction.AVG, 0.0, 100.0, Set.of("host")));
        WriteResult underV1 = write("cpu", Map.of("host", "a"), 150.0, T0);
        assertFalse(underV1.accepted());
        assertEquals(1, underV1.definitionVersion().version());
        // 口径放宽：范围扩到 200，新增允许 az 标签。
        definitions.update("cpu", new MetricDefinition.Spec("%", "v2 0..200",
                AggregateFunction.AVG, 0.0, 200.0, Set.of("host", "az")));
        WriteResult underV2 = write("cpu", Map.of("host", "a", "az", "z1"), 150.0, T0 + 1);
        log.info("[输入] 150.0 在 v1 范围[0,100]下被拒；定义更新 v2 范围[0,200] 后重放 150.0");
        assertTrue(underV2.accepted(), underV2.detail());
        assertEquals(2, underV2.definitionVersion().version());
        log.info("[判定] v1 拒绝(version={}) / v2 接受(version={})，旧数据拒绝、新数据接受，计数 accepted={} rangeReject={}",
                underV1.definitionVersion().version(), underV2.definitionVersion().version(),
                registry.acceptedCount(),
                registry.rejectedCounts().get(RejectReason.DEFINITION_VALUE_OUT_OF_RANGE));
        assertEquals(1L, registry.acceptedCount());
        assertEquals(1L, registry.rejectedCounts().get(RejectReason.DEFINITION_VALUE_OUT_OF_RANGE));
    }
}
