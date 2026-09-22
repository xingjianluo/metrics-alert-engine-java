package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;

/**
 * 一条规则 + 一个标签组合 + 一个规则版本 的告警状态机（可变，整体加锁访问）。
 * 状态迁移为确定语义：
 * - PENDING：命中时 hit 计数 +1，达到 pendingThreshold 迁入 FIRING 并产出触发通知；
 *   未命中时 hit 计数清零（抖动不会累积）。
 * - FIRING：未命中时 miss 计数 +1，达到 resolveThreshold 迁入 RESOLVED 并产出恢复通知；
 *   再次命中时 miss 计数清零（抖动不会错误恢复，也不会重复触发）。
 * - RESOLVED：等同于重新开始，下一次命中进入 PENDING 计数（hit=1）。
 */
public final class AlertStatus {

    private final String ruleId;
    private final int ruleVersion;
    private final LabelSet labels;

    private AlertState state = AlertState.RESOLVED;
    private int consecutiveHits;
    private int consecutiveMisses;
    private long lastFiredWindowStart = Long.MIN_VALUE;
    private long lastNotifyWindowStart = Long.MIN_VALUE;
    private long evaluations;

    public AlertStatus(String ruleId, int ruleVersion, LabelSet labels) {
        this.ruleId = ruleId;
        this.ruleVersion = ruleVersion;
        this.labels = labels;
    }

    public String ruleId() {
        return ruleId;
    }

    public int ruleVersion() {
        return ruleVersion;
    }

    public LabelSet labels() {
        return labels;
    }

    public AlertState state() {
        return state;
    }

    public int consecutiveHits() {
        return consecutiveHits;
    }

    public int consecutiveMisses() {
        return consecutiveMisses;
    }

    public long lastFiredWindowStart() {
        return lastFiredWindowStart;
    }

    public long lastNotifyWindowStart() {
        return lastNotifyWindowStart;
    }

    public long evaluations() {
        return evaluations;
    }

    public String statusKey() {
        // TODO 骨架
        return null;
    }

    /**
     * 应用一个窗口的判定结果，返回本窗口发生的迁移（无迁移时为 null）。
     * 该方法只驱动状态，不处理通知去重与静默期。
     */
    Transition apply(boolean hit, long windowStart) {
        // TODO 骨架
        return null;
    }

    /** 一次状态迁移的不可变描述。 */
    public record Transition(AlertState from, AlertState to, long windowStart, String reason) {
    }
}
