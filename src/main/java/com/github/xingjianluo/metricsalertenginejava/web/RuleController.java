package com.github.xingjianluo.metricsalertenginejava.web;

import com.github.xingjianluo.metricsalertenginejava.engine.AlertEvaluator;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.AlertEvaluation;
import com.github.xingjianluo.metricsalertenginejava.model.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.model.ComparisonOperator;
import com.github.xingjianluo.metricsalertenginejava.model.Notification;
import com.github.xingjianluo.metricsalertenginejava.model.RuleVersion;
import com.github.xingjianluo.metricsalertenginejava.store.AlertStateStore;
import com.github.xingjianluo.metricsalertenginejava.store.NotificationLog;
import com.github.xingjianluo.metricsalertenginejava.store.RuleStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** 规则版本管理 / 告警求值 / 状态与通知查询 HTTP 接口。 */
@RestController
@RequestMapping("/api/rules")
public class RuleController {

    /** 创建 / 更新规则的请求体（版本号由服务端分配）。 */
    public record RuleRequest(String ruleId,
                              String metric,
                              Map<String, String> labelSelectors,
                              long windowMillis,
                              AggregateFunction function,
                              ComparisonOperator operator,
                              double threshold,
                              int forThreshold,
                              int recoveryThreshold,
                              long silenceMillis) {
    }

    public record RuleResponse(AlertRule rule, String note) {
    }

    private final RuleStore ruleStore;
    private final AlertEvaluator evaluator;
    private final AlertStateStore stateStore;
    private final NotificationLog notificationLog;

    public RuleController(RuleStore ruleStore, AlertEvaluator evaluator,
                          AlertStateStore stateStore, NotificationLog notificationLog) {
        this.ruleStore = ruleStore;
        this.evaluator = evaluator;
        this.stateStore = stateStore;
        this.notificationLog = notificationLog;
    }

    @PostMapping
    public ResponseEntity<RuleResponse> register(@RequestBody RuleRequest request) {
        AlertRule rule = ruleStore.register(request.ruleId(), toSpec(request));
        return ResponseEntity.ok(new RuleResponse(rule, "registered as v1"));
    }

    @PutMapping
    public RuleResponse update(@RequestBody RuleRequest request) {
        // 已存在则产生新版本；不存在则自动注册为 v1。
        AlertRule rule;
        try {
            rule = ruleStore.update(request.ruleId(), toSpec(request));
        } catch (IllegalArgumentException notFound) {
            rule = ruleStore.register(request.ruleId(), toSpec(request));
        }
        return new RuleResponse(rule, "now at v" + rule.version().version()
                + "; older versions retained for reproducible history");
    }

    private static RuleStore.RuleSpec toSpec(RuleRequest r) {
        return new RuleStore.RuleSpec(r.metric(), r.labelSelectors(), r.windowMillis(), r.function(),
                r.operator(), r.threshold(), r.forThreshold(), r.recoveryThreshold(), r.silenceMillis());
    }

    @GetMapping("/{ruleId}/versions")
    public List<AlertRule> versions(@PathVariable String ruleId) {
        return ruleStore.versions(ruleId);
    }

    @PostMapping("/evaluate")
    public Map<String, Object> evaluate(@RequestParam long fromWindowStart) {
        List<AlertEvaluation> evaluations = evaluator.evaluateAll(fromWindowStart);
        List<Notification> notifications = evaluator.drainEmittedNotifications();
        return Map.of("evaluations", evaluations, "emittedNotifications", notifications);
    }

    @PostMapping("/{ruleId}/versions/{version}/evaluate")
    public Map<String, Object> evaluateVersion(@PathVariable String ruleId, @PathVariable int version,
                                               @RequestParam long fromWindowStart) {
        AlertRule rule = ruleStore.get(new RuleVersion(ruleId, version));
        List<AlertEvaluation> evaluations = evaluator.evaluateRule(rule, fromWindowStart);
        List<Notification> notifications = evaluator.drainEmittedNotifications();
        return Map.of("evaluations", evaluations, "emittedNotifications", notifications);
    }

    @GetMapping("/{ruleId}/versions/{version}/state")
    public List<AlertStateStore.StateRecord> state(@PathVariable String ruleId, @PathVariable int version) {
        return stateStore.list(new RuleVersion(ruleId, version));
    }

    @GetMapping("/{ruleId}/versions/{version}/history")
    public List<AlertEvaluation> history(@PathVariable String ruleId, @PathVariable int version) {
        return stateStore.history(new RuleVersion(ruleId, version));
    }

    @GetMapping("/notifications")
    public List<Notification> notifications() {
        return notificationLog.all();
    }
}
