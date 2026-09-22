package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.engine.EngineConfig;
import com.github.xingjianluo.metricsalertenginejava.engine.MetricsAlertEngine;
import com.github.xingjianluo.metricsalertenginejava.rule.EvaluationRecord;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** 测试公共工具：可控时钟、固定基准时间、统一的输入/判定日志打印。 */
final class TestSupport {

    static final Logger LOG = LoggerFactory.getLogger("ALERT-TEST");

    /** 固定基准（对齐整分钟）：约为 2026-09-23T00:00:00Z，窗口计算全部基于它，保证确定性。 */
    static final long T0 = 1_789_999_980_000L;
    static final long MINUTE = 60_000L;
    static final long SECOND = 1_000L;

    private TestSupport() {
    }

    /** 可控时钟句柄：注册指标时把“当前时间”推进到数据时间附近，避免未来 60s 上界拒绝样本。 */
    static final class ControlledClock {
        private final AtomicLong now;

        ControlledClock(long start) {
            this.now = new AtomicLong(start);
        }

        long get() {
            return now.get();
        }

        void advanceTo(long millis) {
            now.set(millis);
        }

        void advance(long deltaMillis) {
            now.addAndGet(deltaMillis);
        }
    }

    record EngineWithClock(MetricsAlertEngine engine, ControlledClock clock) {
        /** 注册指标前把时钟放到 t，使合法时间范围覆盖测试数据。 */
        void at(long t) {
            clock.advanceTo(t);
        }
    }

    static EngineWithClock engine(EngineConfig config) {
        ControlledClock clock = new ControlledClock(T0);
        return new EngineWithClock(new MetricsAlertEngine(config, clock::get), clock);
    }

    static EngineWithClock defaultEngine() {
        return engine(EngineConfig.defaults());
    }

    static LabelSet labels(String... kv) {
        if (kv.length % 2 != 0) {
            throw new IllegalArgumentException("kv must be pairs");
        }
        Map<String, String> m = new java.util.LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return LabelSet.of(m);
    }

    static void logInput(String scenario, Object input) {
        LOG.info("[{}] INPUT  >>> {}", scenario, input);
    }

    static void logResult(String scenario, Object result) {
        LOG.info("[{}] RESULT <<< {}", scenario, result);
    }

    static void logEvaluation(String scenario, EvaluationRecord r) {
        LOG.info("[{}] JUDGE  --- {}", scenario, r.explain());
    }

    static Map<String, String> mapOf(String... kv) {
        Map<String, String> m = new java.util.LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }
}
