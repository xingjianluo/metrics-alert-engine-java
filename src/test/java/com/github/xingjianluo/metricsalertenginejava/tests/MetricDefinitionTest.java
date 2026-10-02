package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.MetricDefinitionLimitException;
import com.github.xingjianluo.metricsalertenginejava.engine.MetricDefinitionService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 指标定义：创建/更新版本化、旧版本可永久取回、容量上限显式拒绝并计数。 */
class MetricDefinitionTest {

    private static final Logger log = LoggerFactory.getLogger(MetricDefinitionTest.class);
    private static final long T0 = 1_000_000L;

    private ManualClock clock;
    private MetricDefinitionStore store;
    private MetricsRegistry registry;
    private MetricDefinitionService service;

    @BeforeEach
    void setUp() {
        clock = new ManualClock(T0);
        store = new MetricDefinitionStore();
        registry = new MetricsRegistry();
        EngineProperties props = TestSupport.properties();
        service = new MetricDefinitionService(store, registry, props, clock);
    }

    private MetricDefinition.Spec spec(AggregateFunction fn, Double min, Double max,
                                       Set<String> keys) {
        return new MetricDefinition.Spec("%", "cpu usage percent", fn, min, max, keys);
    }

    @Test
    void createThenUpdateProducesImmutableVersions() {
        MetricDefinition v1 = service.register("cpu",
                spec(AggregateFunction.AVG, 0.0, 100.0, Set.of("host", "region")));
        log.info("[定义] 首次登记 cpu -> {} 口径={} 范围={} 标签白名单={}",
                v1.version(), v1.defaultAggregation(), v1.rangeDescription(), v1.allowedLabelKeys());
        assertEquals(new MetricVersion("cpu", 1), v1.version());
        assertEquals(AggregateFunction.AVG, v1.defaultAggregation());

        clock.advance(5_000L);
        MetricDefinition v2 = service.update("cpu",
                spec(AggregateFunction.MAX, 0.0, 200.0, Set.of("host", "region", "az")));
        log.info("[定义] 更新 cpu -> {} 口径={} 范围={} 标签白名单={}",
                v2.version(), v2.defaultAggregation(), v2.rangeDescription(), v2.allowedLabelKeys());
        assertEquals(2, v2.version().version());
        assertNotEquals(v1.createdAtMillis(), v2.createdAtMillis());

        // 旧版本不被覆盖，仍可按版本号取回当时口径。
        MetricDefinition fetchedV1 = service.get(new MetricVersion("cpu", 1));
        log.info("[定义] 取回旧版本 {} 口径仍为={} 范围仍为={}",
                fetchedV1.version(), fetchedV1.defaultAggregation(), fetchedV1.rangeDescription());
        assertEquals(AggregateFunction.AVG, fetchedV1.defaultAggregation());
        assertEquals(100.0, fetchedV1.maxValue());
        assertEquals(v1, fetchedV1);

        List<MetricDefinition> all = service.versions("cpu");
        assertEquals(2, all.size());
        assertEquals(List.of(v1, v2), all);
        assertEquals(2, service.latest("cpu").version().version());
        assertEquals(1L, registry.definitionsCreatedCount());
        assertEquals(1L, registry.definitionsUpdatedCount());
        assertEquals(0L, registry.definitionsRejectedCount());
        log.info("[判定] 创建计数={} 更新计数={} 拒绝计数={} 版本数={}",
                registry.definitionsCreatedCount(), registry.definitionsUpdatedCount(),
                registry.definitionsRejectedCount(), all.size());
    }

    @Test
    void duplicateRegisterRejectedButUpdateGrowsVersions() {
        service.register("cpu", spec(AggregateFunction.AVG, null, null, Set.of()));
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> service.register("cpu", spec(AggregateFunction.SUM, null, null, Set.of())));
        log.info("[判定] 重复 register 被拒绝 依据={}", ex.getMessage());
        assertEquals(1, service.versions("cpu").size());

        MetricDefinition v2 = service.registerOrUpdate("cpu",
                spec(AggregateFunction.SUM, null, null, Set.of()));
        assertEquals(2, v2.version().version());
        MetricDefinition v1ViaUpsert = service.registerOrUpdate("mem",
                new MetricDefinition.Spec("bytes", null, AggregateFunction.SUM, null, null, null));
        assertEquals(1, v1ViaUpsert.version().version());
        log.info("[判定] upsert 已存在指标->v{}，不存在指标 mem->v1", v2.version().version());
    }

    @Test
    void invalidSpecRejectedBeforeAnyVersionStored() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> service.register("cpu",
                        spec(AggregateFunction.AVG, 100.0, 0.0, Set.of())));
        log.info("[判定] min>max 被拒绝 依据={}", ex.getMessage());
        assertTrue(ex.getMessage().contains("must not exceed"));
        IllegalArgumentException badKey = assertThrows(IllegalArgumentException.class,
                () -> service.register("cpu",
                        new MetricDefinition.Spec(null, null, AggregateFunction.SUM,
                                null, null, Set.of("1bad"))));
        log.info("[判定] 非法标签键白名单被拒绝 依据={}", badKey.getMessage());
        assertNull(service.latest("cpu"));
        assertEquals(0, service.metricCount());
    }

    @Test
    void versionLimitExceededIsExplicitAndCounted() {
        EngineProperties props = TestSupport.properties();
        props.setMaxDefinitionVersionsPerMetric(3);
        service = new MetricDefinitionService(store, registry, props, clock);
        service.register("cpu", spec(AggregateFunction.AVG, null, null, Set.of()));
        service.update("cpu", spec(AggregateFunction.AVG, null, null, Set.of()));
        MetricDefinition v3 = service.update("cpu", spec(AggregateFunction.AVG, null, null, Set.of()));
        log.info("[输入] 版本上限=3，已登记到 {} 后再次更新", v3.version());

        MetricDefinitionLimitException ex = assertThrows(MetricDefinitionLimitException.class,
                () -> service.update("cpu", spec(AggregateFunction.MAX, null, null, Set.of())));
        assertEquals(RejectReason.DEFINITION_LIMIT_EXCEEDED, ex.reason());
        log.info("[判定] 第 4 版被拒绝 reason={} 依据={}", ex.reason(), ex.getMessage());
        assertEquals(3, service.versions("cpu").size());
        assertEquals(1L, registry.definitionsRejectedCount());
        // 旧版本与最新版本都原样保留。
        assertEquals(3, service.latest("cpu").version().version());
        assertEquals(AggregateFunction.AVG, service.latest("cpu").defaultAggregation());
    }

    @Test
    void metricDefinitionCountLimitExceededIsExplicitAndCounted() {
        EngineProperties props = TestSupport.properties();
        props.setMaxMetricDefinitions(2);
        service = new MetricDefinitionService(store, registry, props, clock);
        service.register("cpu", spec(AggregateFunction.AVG, null, null, Set.of()));
        service.register("mem", spec(AggregateFunction.SUM, null, null, Set.of()));
        log.info("[输入] 指标定义数上限=2，已登记 cpu,mem，再登记 disk");
        MetricDefinitionLimitException ex = assertThrows(MetricDefinitionLimitException.class,
                () -> service.register("disk", spec(AggregateFunction.SUM, null, null, Set.of())));
        assertEquals(RejectReason.DEFINITION_LIMIT_EXCEEDED, ex.reason());
        log.info("[判定] 第 3 个指标被拒绝 reason={} 依据={}", ex.reason(), ex.getMessage());
        assertEquals(2, service.metricCount());
        assertEquals(1L, registry.definitionsRejectedCount());
        assertEquals(0L, registry.definitionsCreatedCount() - 2);
    }

    @Test
    void unknownVersionFetchFailsButUnknownLatestIsNull() {
        assertThrows(IllegalArgumentException.class,
                () -> service.get(new MetricVersion("ghost", 1)));
        assertNull(service.latest("ghost"));
        log.info("[判定] 未登记指标 latest=null（宽松写入依据），显式取版本则报错");
    }
}
