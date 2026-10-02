package com.github.xingjianluo.metricsalertenginejava.web;

import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.store.MetricDefinitionStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** 指标定义（语义口径）登记 / 版本管理 HTTP 接口。 */
@RestController
@RequestMapping("/api/metric-definitions")
public class MetricDefinitionController {

    /** 登记 / 更新定义的请求体（版本号由服务端分配）。 */
    public record DefinitionRequest(String metric,
                                    String unit,
                                    String description,
                                    AggregateFunction defaultFunction,
                                    Double minValue,
                                    Double maxValue,
                                    Set<String> allowedLabelKeys) {
    }

    public record DefinitionResponse(MetricDefinition definition, String note) {
    }

    private final MetricDefinitionStore store;

    public MetricDefinitionController(MetricDefinitionStore store) {
        this.store = store;
    }

    @PostMapping
    public ResponseEntity<?> register(@RequestBody DefinitionRequest request) {
        try {
            MetricDefinition def = store.register(request.metric(), toSpec(request));
            return ResponseEntity.ok(new DefinitionResponse(def, "registered as v1"));
        } catch (MetricDefinitionStore.DefinitionLimitException limit) {
            // 容量超限：明确拒绝（429），可观测计数已累加。
            return ResponseEntity.status(429).body(Map.of("error", limit.getMessage()));
        } catch (IllegalArgumentException | IllegalStateException bad) {
            return ResponseEntity.unprocessableEntity().body(Map.of("error", bad.getMessage()));
        }
    }

    @PutMapping
    public ResponseEntity<?> update(@RequestBody DefinitionRequest request) {
        try {
            MetricDefinition def;
            try {
                def = store.update(request.metric(), toSpec(request));
            } catch (IllegalArgumentException notFound) {
                def = store.register(request.metric(), toSpec(request));
            }
            return ResponseEntity.ok(new DefinitionResponse(def, "now at v" + def.version()
                    + "; older versions retained for reproducible interpretation"));
        } catch (MetricDefinitionStore.DefinitionLimitException limit) {
            return ResponseEntity.status(429).body(Map.of("error", limit.getMessage()));
        } catch (IllegalArgumentException | IllegalStateException bad) {
            return ResponseEntity.unprocessableEntity().body(Map.of("error", bad.getMessage()));
        }
    }

    @GetMapping("/{metric}/versions")
    public List<MetricDefinition> versions(@PathVariable String metric) {
        return store.versions(metric);
    }

    @GetMapping("/{metric}/versions/{version}")
    public MetricDefinition version(@PathVariable String metric, @PathVariable int version) {
        return store.get(metric, version);
    }

    private static MetricDefinitionStore.DefinitionSpec toSpec(DefinitionRequest r) {
        return new MetricDefinitionStore.DefinitionSpec(r.unit(), r.description(),
                r.defaultFunction(), r.minValue(), r.maxValue(), r.allowedLabelKeys());
    }
}
