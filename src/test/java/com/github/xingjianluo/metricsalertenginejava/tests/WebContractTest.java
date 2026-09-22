package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.engine.MetricsAlertEngine;
import com.github.xingjianluo.metricsalertenginejava.web.MetricsController;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 控制器契约测试（不起容器）：验证 JSON 序列化与拒绝状态码。 */
class WebContractTest {

    private final MetricsAlertEngine engine = MetricsAlertEngine.create(
            com.github.xingjianluo.metricsalertenginejava.engine.EngineConfig.defaults());
    private final MockMvc mvc;

    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mvc = MockMvcBuilders.standaloneSetup(new MetricsController(engine))
                .setMessageConverters(new MappingJackson2HttpMessageConverter(mapper))
                .build();
    }

    @Test
    void registerMetricReturnsJson() throws Exception {
        mvc.perform(post("/api/metrics")
                        .contentType("application/json")
                        .content("{\"name\":\"cpu\",\"labelKeys\":[\"host\"],\"cardinality\":10}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("cpu"))
                .andExpect(jsonPath("$.version").value(1));
    }

    @Test
    void writeRejectionReturns422WithReason() throws Exception {
        mvc.perform(post("/api/metrics")
                        .contentType("application/json")
                        .content("{\"name\":\"cpu\",\"labelKeys\":[\"host\"],\"cardinality\":10}"))
                .andExpect(status().isOk());
        mvc.perform(post("/api/metrics/cpu/write")
                        .contentType("application/json")
                        .content("{\"labels\":{\"pod\":\"x\"},\"value\":1,\"timestampMillis\":"
                                + (System.currentTimeMillis()) + "}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("INVALID_LABEL"));
    }

    @Test
    void queryWithLabelDottedParamsIsolatesSeries() throws Exception {
        long now = System.currentTimeMillis();
        mvc.perform(post("/api/metrics")
                        .contentType("application/json")
                        .content("{\"name\":\"cpu\",\"labelKeys\":[\"host\"],\"cardinality\":10}"))
                .andExpect(status().isOk());
        mvc.perform(post("/api/metrics/cpu/write")
                        .contentType("application/json")
                        .content("{\"labels\":{\"host\":\"a\"},\"value\":5,\"timestampMillis\":" + now + "}"))
                .andExpect(status().isOk());
        mvc.perform(post("/api/metrics/cpu/write")
                        .contentType("application/json")
                        .content("{\"labels\":{\"host\":\"b\"},\"value\":9,\"timestampMillis\":" + now + "}"))
                .andExpect(status().isOk());
        long w = now - Math.floorMod(now, 60_000L);
        mvc.perform(get("/api/metrics/cpu/query")
                        .param("windowStart", String.valueOf(w))
                        .param("windowSize", "60000")
                        .param("label.host", "a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.labels.host").value("a"))
                .andExpect(jsonPath("$.functions.SUM").value(5.0));
        mvc.perform(get("/api/metrics/cpu/query")
                        .param("windowStart", String.valueOf(w))
                        .param("windowSize", "60000")
                        .param("label.host", "b"))
                .andExpect(jsonPath("$.functions.SUM").value(9.0));
    }

    @Test
    void dropsEndpointReturnsCounters() throws Exception {
        mvc.perform(get("/api/drops"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").exists());
    }
}
