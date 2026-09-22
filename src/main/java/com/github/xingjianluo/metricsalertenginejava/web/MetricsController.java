package com.github.xingjianluo.metricsalertenginejava.web;

import com.github.xingjianluo.metricsalertenginejava.engine.MetricsAlertEngine;
import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WindowAggregate;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.rule.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.rule.EvaluationRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 演示用 HTTP 接口。核心能力均在引擎层，控制器只做薄封装：
 * POST /api/metrics               注册指标
 * POST /api/metrics/{name}/write  写入样本（被拒绝返回 422 与可区分 reason）
 * GET  /api/metrics/{name}/query  窗口聚合查询
 * POST /api/rules                 注册规则
 * POST /api/rules/{id}/evaluate   同步评估若干连续窗口
 * GET  /api/rules/{id}/state      查看当前状态与历史结论
 */
@RestController
@RequestMapping("/api")
public class MetricsController {

    private final MetricsAlertEngine engine;

    public MetricsController(MetricsAlertEngine engine) {
        this.engine = engine;
    }

    public record MetricRequest(String name, Set<String> labelKeys, Integer cardinality,
                                Long retentionMillis) {
    }

    public record WriteRequest(Map<String, String> labels, double value, Long timestampMillis) {
    }

    public record RuleRequest(String id, String metric, String aggregator, String operator, double threshold,
                              Integer pendingThreshold, Integer resolveThreshold, Long windowSizeMillis,
                              Long silenceMillis, Set<String> labelFilter) {
    }

    public record EvaluateRequest(Map<String, String> labels, Long fromWindowStart,
                                  Long toWindowStartExclusive) {
    }

    @PostMapping("/metrics")
    public MetricDefinition registerMetric(@RequestBody MetricRequest request) {
        long retention = request.retentionMillis() == null
                ? engine.config().defaultRetentionMillis() : request.retentionMillis();
        int cardinality = request.cardinality() == null ? engine.config().maxSeriesPerMetric()
                : request.cardinality();
        return engine.registerMetric(request.name(),
                request.labelKeys() == null ? Set.of() : request.labelKeys(), cardinality, retention);
    }

    @PostMapping("/metrics/{name}/write")
    public ResponseEntity<WriteResult> write(@PathVariable String name, @RequestBody WriteRequest request) {
        long ts = request.timestampMillis() == null ? System.currentTimeMillis() : request.timestampMillis();
        Sample sample = new Sample(name, LabelSet.of(request.labels() == null ? Map.of() : request.labels()),
                request.value(), ts);
        WriteResult result = engine.write(sample);
        return ResponseEntity.status(result.accepted() ? HttpStatus.OK : HttpStatus.UNPROCESSABLE_ENTITY)
                .body(result);
    }

    @GetMapping("/metrics/{name}/query")
    public WindowAggregate query(@PathVariable String name,
                                 @RequestParam(required = false) Map<String, String> labels,
                                 @RequestParam long windowStart, @RequestParam long windowSize,
                                 @RequestParam(required = false) Integer metricVersion) {
        LabelSet labelSet = LabelSet.of(labels == null ? Map.of() : new LinkedHashMap<>(labels));
        List<Aggregator> functions = List.of(Aggregator.values());
        return engine.queryWindow(name, labelSet, windowStart, windowSize, metricVersion, functions);
    }

    @PostMapping("/rules")
    public AlertRule registerRule(@RequestBody RuleRequest request) {
        return engine.registerRule(request.id(), request.metric(),
                Aggregator.valueOf(request.aggregator()), AlertRule.Operator.valueOf(request.operator()),
                request.threshold(),
                request.pendingThreshold() == null ? 1 : request.pendingThreshold(),
                request.resolveThreshold() == null ? 1 : request.resolveThreshold(),
                request.windowSizeMillis(),
                request.silenceMillis() == null ? 0L : request.silenceMillis(),
                request.labelFilter() == null ? Set.of() : request.labelFilter());
    }

    @PostMapping("/rules/{id}/evaluate")
    public List<EvaluationRecord> evaluate(@PathVariable String id, @RequestBody EvaluateRequest request) {
        LabelSet labelSet = LabelSet.of(request.labels() == null ? Map.of() : request.labels());
        return engine.evaluateRange(id, labelSet, request.fromWindowStart(),
                request.toWindowStartExclusive(), null, null);
    }

    @GetMapping("/rules/{id}/state")
    public Map<String, Object> state(@PathVariable String id,
                                     @RequestParam(required = false) Map<String, String> labels,
                                     @RequestParam(required = false) Integer version) {
        LabelSet labelSet = LabelSet.of(labels == null ? Map.of() : new LinkedHashMap<>(labels));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("state", engine.alertState(id, version, labelSet));
        out.put("evaluations", engine.evaluationHistory(id, version, labelSet));
        out.put("notifications", engine.notificationHistory(id, version, labelSet));
        return out;
    }

    @GetMapping("/drops")
    public Map<String, Object> drops() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("counters", engine.dropStats().snapshot());
        out.put("total", engine.dropStats().totalDropped());
        out.put("evalQueueSize", engine.evalQueueSize());
        return out;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleIllegalArgument(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> handleIllegalState(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
    }
}
