package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.model.AlertEvaluation;
import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.Notification;
import com.github.xingjianluo.metricsalertenginejava.store.NotificationLog;

/**
 * 通知去重与静默：
 * <ul>
 *   <li>仅 PENDING→FIRING（首次触发）与 RECOVERING→NORMAL（恢复完成）两类状态迁移产生通知；</li>
 *   <li>去重维度是 (ruleId, 标签组合)，跨规则版本保持一致：规则更新产生新版本不会重复通知同一组合；</li>
 *   <li>静默期内只抑制通知，状态记录照常写入；抑制也会登记日志，便于审计；</li>
 *   <li>静默判断只看最近一次“实际发出”的通知，被抑制的通知不延长静默窗口。</li>
 * </ul>
 */
public class NotificationService {

    private final NotificationLog log;
    private final EngineClock clock;

    public NotificationService(NotificationLog log, EngineClock clock) {
        this.log = log;
        this.clock = clock;
    }

    /** 对一次求值结论决定是否发出通知；非迁移性求值返回 null。 */
    public Notification onEvaluation(AlertEvaluation evaluation, long silenceMillis) {
        Notification.Type type = notificationType(evaluation);
        if (type == null) {
            return null;
        }
        long now = clock.nowMillis();
        long lastEmitted = log.lastEmittedMillis(evaluation.ruleVersion(), evaluation.labels());
        boolean suppressed = lastEmitted >= 0 && (now - lastEmitted) < silenceMillis;
        String detail = "transition " + evaluation.fromState() + "->" + evaluation.toState()
                + " on " + evaluation.ruleVersion() + " " + evaluation.labels()
                + " windowStart=" + evaluation.windowStart()
                + " value=" + evaluation.aggregatedValue()
                + " conditionMet=" + evaluation.conditionMet()
                + (suppressed ? " [SUPPRESSED by silence window]" : " [EMITTED]");
        Notification notification = new Notification(evaluation.ruleVersion(), evaluation.metric(),
                evaluation.labels(), type, now, suppressed, detail);
        log.append(notification);
        return notification;
    }

    private static Notification.Type notificationType(AlertEvaluation e) {
        if (e.fromState() == AlertState.PENDING && e.toState() == AlertState.FIRING) {
            return Notification.Type.FIRING;
        }
        if (e.fromState() == AlertState.RECOVERING && e.toState() == AlertState.NORMAL) {
            return Notification.Type.RECOVERED;
        }
        return null;
    }
}
