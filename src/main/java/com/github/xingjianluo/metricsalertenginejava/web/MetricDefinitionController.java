package com.github.xingjianluo.metricsalertenginejava.web;

import com.github.xingjianluo.metricsalertenginejava.engine.MetricDefinitionLimitException;
import com.github.xingjianluo.metricsalertenginejava.engine.MetricDefinitionService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;
import org.springframework.http.HttpStatus;
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

/** 指标定义登记 / 更新 / 按版本取回 HTTP 接口。 */
@RestController
@RequestMapping("/api/metric-definitions")
public class MetricDefinitionController {

    /** 定义请求体；版本号与创建时间由服务端分配。 */
    public record DefinitionRequest(String metric,
                                    String unit,
                                    String description,
                                    AggregateFunction defaultAggregation,
                                    Double minValue,
                                    Double maxValue,
                                    Set<String> allowedLabelKeys) {
    }

    public record DefinitionResponse(MetricDefinition definition, String note) {
    }

    public record ErrorBody(String error, String reason) {
    }

    private final MetricDefinitionService service;

    public MetricDefinitionController(MetricDefinitionService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<?> register(@RequestBody DefinitionRequest request) {
        try {
            MetricDefinition definition = service.register(request.metric(), toSpec(request));
            return ResponseEntity.ok(new DefinitionResponse(definition, "registered as v1"));
        } catch (MetricDefinitionLimitException e) {
            return limitResponse(e);
        }
    }

    @PutMapping
    public ResponseEntity<?> upsert(@RequestBody DefinitionRequest request) {
        try {
            // 存在则产生新版本，不存在则登记为 v1。
            MetricDefinition definition = service.registerOrUpdate(request.metric(), toSpec(request));
            String note = definition.version().version() == 1
                    ? "registered as v1"
                    : "now at v" + definition.version().version()
                            + "; older versions retained for reproducible interpretation";
            return ResponseEntity.ok(new DefinitionResponse(definition, note));
        } catch (MetricDefinitionLimitException e) {
            return limitResponse(e);
        }
    }

    @GetMapping("/{metric}/versions")
    public List<MetricDefinition> versions(@PathVariable String metric) {
        return service.versions(metric);
    }

    @GetMapping("/{metric}/versions/{version}")
    public MetricDefinition version(@PathVariable String metric, @PathVariable int version) {
        return service.get(new MetricVersion(metric, version));
    }

    @GetMapping("/{metric}/latest")
    public MetricDefinition latest(@PathVariable String metric) {
        MetricDefinition latest = service.latest(metric);
        if (latest == null) {
            throw new IllegalArgumentException("metric definition not found: " + metric);
        }
        return latest;
    }

    private static MetricDefinition.Spec toSpec(DefinitionRequest r) {
        return new MetricDefinition.Spec(r.unit(), r.description(), r.defaultAggregation(),
                r.minValue(), r.maxValue(), r.allowedLabelKeys());
    }

    private static ResponseEntity<Map<String, Object>> limitResponse(MetricDefinitionLimitException e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of(
                "error", e.getMessage(),
                "reason", e.reason().name()));
    }
}
