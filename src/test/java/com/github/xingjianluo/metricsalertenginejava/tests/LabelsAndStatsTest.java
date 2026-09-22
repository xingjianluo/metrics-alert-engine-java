package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.WindowStats;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 标签合法性、规范串确定性与窗口统计 / 去重。 */
class LabelsAndStatsTest {

    private static final Logger log = LoggerFactory.getLogger(LabelsAndStatsTest.class);

    @Test
    void labelsAreCanonicalAndValidated() {
        Labels a = Labels.of(Map.of("host", "a", "service", "x"));
        Labels b = Labels.of(Map.of("service", "x", "host", "a"));
        log.info("[输入] host=a,service=x（两种插入顺序） [判定] canonical={}", a.canonicalString());
        assertEquals(a, b, "不同插入顺序应得到相等标签集");
        assertEquals("{host=\"a\",service=\"x\"}", a.canonicalString(), "规范串必须按 key 排序");

        assertThrows(IllegalArgumentException.class, () -> Labels.of(Map.of("1bad", "v")),
                "非法标签名必须被拒绝");
        assertThrows(IllegalArgumentException.class, () -> Labels.of(Map.of("k", "")),
                "空标签值必须被拒绝");
        log.info("[判定] 非法标签名/空值均抛出 IllegalArgumentException，拒绝原因可区分");
    }

    @Test
    void windowStatsDeduplicatesByTimestamp() {
        WindowStats stats = new WindowStats(0L, 1000L);
        assertTrue(stats.add(10, 100));
        assertTrue(stats.add(20, 200));
        assertFalse(stats.add(999, 100), "同一时间戳重复写入必须返回 false，不双计");
        log.info("[输入] 值 10@100, 20@200, 重复 999@100 [判定] count={},sum={},avg={}",
                stats.count(), stats.sum(), stats.avg());
        assertEquals(2, stats.count());
        assertEquals(30, stats.sum(), 1e-9);
        assertEquals(15, stats.avg(), 1e-9);
        assertEquals(20, AggregateFunction.MAX.apply(stats), 1e-9);
        assertEquals(10, AggregateFunction.MIN.apply(stats), 1e-9);
    }
}
