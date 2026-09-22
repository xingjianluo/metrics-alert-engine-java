package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;
import com.github.xingjianluo.metricsalertenginejava.model.WindowAggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * 告警求值器：管理全部 (规则版本, 标签组合) 的状态机、求值记录与通知结论。
 *
 * 关键保证：
 * <ul>
 *   <li>同一 (规则版本, 标签) 的窗口必须按起点严格递增且不重复求值，否则抛出
 *       IllegalStateException——状态推进只能按时间顺序确定发生；</li>
 *   <li>通知去重：FIRING/RESOLVED 状态机本身保证不重复触发；此外按
 *       dedupKey=(ruleId@version, labels, notifyState) 记忆最近一次实际发出时间；</li>
 *   <li>静默期：处于 silenceMillis 内的通知被标记 suppressed=true（仍记录迁移），
 *       只抑制发送、不改变状态；</li>
 *   <li>全部历史在同一把锁下追加，查询返回快照副本。</li>
 * </ul>
 */
public final class AlertEvaluator {

    private final Map<String, AlertStatus> statuses = new HashMap<>();
    private final List<EvaluationRecord> evaluations = new ArrayList<>();
    private final List<NotificationRecord> notifications = new ArrayList<>();
    private final Map<String, Long> lastNotifiedAtByDedupKey = new HashMap<>();
    private final LongSupplier clock;
    private final Object lock = new Object();

    public AlertEvaluator(LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * 对一条规则的一个窗口聚合结果求值。调用方保证按 windowStart 递增调用。
     * 返回求值记录与可能的通知（无状态迁移时 notification 为 null）。
     */
    public EvaluationOutcome evaluate(AlertRule rule, LabelSet labels, WindowAggregate aggregate,
                                      long windowStart) {
        if (windowStart % rule.windowSizeMillis() != 0) {
            throw new IllegalArgumentException(
                    "window.misaligned: windowStart " + windowStart + " not aligned to size "
                            + rule.windowSizeMillis());
        }
        synchronized (lock) {
            AlertStatus status = statuses.computeIfAbsent(
                    rule.id() + "@v" + rule.version() + labels.seriesKey(),
                    k -> new AlertStatus(rule.id(), rule.version(), labels));

            if (windowStart <= status.lastEvaluatedWindowStart()) {
                throw new IllegalStateException(
                        "window.out.of.order: rule=" + rule.id() + " v" + rule.version() + " labels=" + labels
                                + " window=" + windowStart + " lastEvaluated="
                                + status.lastEvaluatedWindowStart());
            }

            AlertState before = status.state();
            Double input = aggregate == null ? null : aggregate.get(rule.aggregator());
            // 空窗口的 AVG/MIN/MAX 为 null：无观测值不构成命中；COUNT/SUM 为 0 仍正常比较。
            boolean hit = input != null && rule.operator().test(input, rule.threshold());

            AlertStatus.Transition transition =
                    status.apply(hit, windowStart, rule.pendingThreshold(), rule.resolveThreshold());

            EvaluationRecord record = new EvaluationRecord(rule.id(), rule.version(), labels, windowStart,
                    input, rule.threshold(), hit, before, status.state(),
                    transition == null ? null : transition.reason());
            evaluations.add(record);

            NotificationRecord notification = null;
            if (transition != null) {
                String dedupKey = dedupKey(rule, labels, transition.to());
                long now = clock.getAsLong();
                Long lastSent = lastNotifiedAtByDedupKey.get(dedupKey);
                boolean suppressed = lastSent != null && (now - lastSent) < rule.silenceMillis();
                String message = buildMessage(rule, labels, transition, input, suppressed);
                notification = new NotificationRecord(rule.id(), rule.version(), labels, transition.to(),
                        windowStart, now, suppressed, dedupKey, message);
                notifications.add(notification);
                if (!suppressed) {
                    lastNotifiedAtByDedupKey.put(dedupKey, now);
                }
            }
            return new EvaluationOutcome(record, notification);
        }
    }

    private static String dedupKey(AlertRule rule, LabelSet labels, AlertState notifyState) {
        return rule.id() + "@v" + rule.version() + labels.seriesKey() + "#" + notifyState;
    }

    private static String buildMessage(AlertRule rule, LabelSet labels, AlertStatus.Transition transition,
                                       Double input, boolean suppressed) {
        String verb = transition.to() == AlertState.FIRING ? "FIRED" : "RESOLVED";
        String base = verb + " rule=" + rule.id() + "@v" + rule.version() + " labels=" + labels
                + " window=" + transition.windowStart() + " observed=" + input
                + " condition=[" + rule.aggregator() + " " + rule.operator() + " " + rule.threshold() + "]"
                + " because: " + transition.reason();
        return suppressed ? base + " [SUPPRESSED by silence window]" : base;
    }

    public List<EvaluationRecord> evaluationHistory(String ruleId, Integer version, LabelSet labels) {
        synchronized (lock) {
            List<EvaluationRecord> out = new ArrayList<>();
            for (EvaluationRecord r : evaluations) {
                if (r.ruleId().equals(ruleId)
                        && (version == null || r.ruleVersion() == version)
                        && (labels == null || r.labels().equals(labels))) {
                    out.add(r);
                }
            }
            return List.copyOf(out);
        }
    }

    public List<NotificationRecord> notificationHistory(String ruleId, Integer version, LabelSet labels) {
        synchronized (lock) {
            List<NotificationRecord> out = new ArrayList<>();
            for (NotificationRecord n : notifications) {
                if (n.ruleId().equals(ruleId)
                        && (version == null || n.ruleVersion() == version)
                        && (labels == null || n.labels().equals(labels))) {
                    out.add(n);
                }
            }
            return List.copyOf(out);
        }
    }

    public AlertState currentState(String ruleId, int version, LabelSet labels) {
        synchronized (lock) {
            AlertStatus status = statuses.get(ruleId + "@v" + version + labels.seriesKey());
            return status == null ? AlertState.RESOLVED : status.state();
        }
    }

    /** 当前每个 (规则版本, 标签) 最近求值窗口，测试可观测使用。 */
    public Map<String, Long> lastEvaluatedWindows() {
        synchronized (lock) {
            Map<String, Long> out = new HashMap<>();
            for (AlertStatus s : statuses.values()) {
                out.put(s.statusKey(), s.lastEvaluatedWindowStart());
            }
            return Map.copyOf(out);
        }
    }

    /** 一次求值的产物：求值记录 + 可能的通知（无状态迁移时为 null）。 */
    public record EvaluationOutcome(EvaluationRecord record, NotificationRecord notification) {
    }
}
