package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.WindowAggregate;

import java.util.List;
import java.util.Map;

/**
 * 告警求值器：管理全部 (规则版本, 标签组合) 的状态机、求值记录与通知结论。
 * 状态迁移必须按窗口时间顺序逐窗口进行（乱序求值将被拒绝），保证确定语义；
 * 通知按 dedupKey 去重，静默期只抑制通知不改变状态。
 */
public final class AlertEvaluator {

    private final Map<String, AlertStatus> statuses = new java.util.HashMap<>();
    private final List<EvaluationRecord> evaluations = new java.util.ArrayList<>();
    private final List<NotificationRecord> notifications = new java.util.ArrayList<>();
    private final Map<String, Long> lastNotifiedAtByDedupKey = new java.util.HashMap<>();
    private final java.util.function.LongSupplier clock;
    private final Object lock = new Object();

    public AlertEvaluator(java.util.function.LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * 对一条规则的一个窗口聚合结果求值。调用方保证按 windowStart 递增调用。
     * 返回求值记录（同时可能附带 0 或 1 条通知记录）。
     */
    public EvaluationOutcome evaluate(AlertRule rule, LabelSet labels, WindowAggregate aggregate,
                                      long windowStart) {
        // TODO 骨架
        return null;
    }

    /** 最近一次被求值的窗口起点（乱序防护）。 */
    public Map<String, Long> lastEvaluatedWindows() {
        // TODO 骨架
        return Map.of();
    }

    public List<EvaluationRecord> evaluationHistory(String ruleId, Integer version, LabelSet labels) {
        // TODO 骨架
        return List.of();
    }

    public List<NotificationRecord> notificationHistory(String ruleId, Integer version, LabelSet labels) {
        // TODO 骨架
        return List.of();
    }

    public AlertState currentState(String ruleId, int version, LabelSet labels) {
        // TODO 骨架
        return null;
    }

    /** 一次求值的产物：求值记录 + 可能的通知。 */
    public record EvaluationOutcome(EvaluationRecord record, NotificationRecord notification) {
    }
}
