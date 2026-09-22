package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;

/**
 * 一条规则 + 一个标签组合 + 一个规则版本 的告警状态机（可变，由 AlertEvaluator 在统一锁下访问）。
 *
 * 确定的状态迁移语义（每个窗口恰好按时间顺序应用一次）：
 * <ul>
 *   <li>RESOLVED + 命中：hits=1，迁入 PENDING；RESOLVED + 未命中：保持 RESOLVED。</li>
 *   <li>PENDING + 命中：hits 加 1；达到 pendingThreshold 时迁入 FIRING（触发）；
 *       PENDING + 未命中：hits 清零，留在 PENDING（抖动不会累积成触发）。</li>
 *   <li>FIRING + 命中：misses 清零，留在 FIRING（抖动不会重复触发）；
 *       FIRING + 未命中：misses 加 1，hits 清零；达到 resolveThreshold 时迁入 RESOLVED（恢复）。</li>
 * </ul>
 * 该类只驱动状态，不处理通知去重与静默期（由 AlertEvaluator 负责）。
 */
public final class AlertStatus {

    private final String ruleId;
    private final int ruleVersion;
    private final LabelSet labels;

    private AlertState state = AlertState.RESOLVED;
    private int consecutiveHits;
    private int consecutiveMisses;
    private long lastFiredWindowStart = Long.MIN_VALUE;
    private long lastEvaluatedWindowStart = Long.MIN_VALUE;
    private final long createdAtMillis;
    private long evaluations;

    public AlertStatus(String ruleId, int ruleVersion, LabelSet labels, long createdAtMillis) {
        this.ruleId = ruleId;
        this.ruleVersion = ruleVersion;
        this.labels = labels;
        this.createdAtMillis = createdAtMillis;
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

    public long lastEvaluatedWindowStart() {
        return lastEvaluatedWindowStart;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    public long evaluations() {
        return evaluations;
    }

    public String statusKey() {
        return ruleId + "@v" + ruleVersion + labels.seriesKey();
    }

    /**
     * 应用一个窗口的判定结果，返回本窗口发生的状态迁移（无迁移时为 null）。
     * 调用方必须保证 windowStart 严格递增且同一窗口不重复应用（AlertEvaluator 负责校验）。
     */
    Transition apply(boolean hit, long windowStart, int pendingThreshold, int resolveThreshold) {
        evaluations++;
        lastEvaluatedWindowStart = windowStart;
        Transition transition = null;
        switch (state) {
            case RESOLVED -> {
                if (hit) {
                    consecutiveHits = 1;
                    consecutiveMisses = 0;
                    if (pendingThreshold == 1) {
                        transition = new Transition(AlertState.RESOLVED, AlertState.FIRING, windowStart,
                                "first hit reached pending threshold 1");
                        state = AlertState.FIRING;
                        lastFiredWindowStart = windowStart;
                    } else {
                        state = AlertState.PENDING;
                    }
                }
                // 未命中：保持 RESOLVED，计数维持 0
            }
            case PENDING -> {
                if (hit) {
                    consecutiveHits++;
                    consecutiveMisses = 0;
                    if (consecutiveHits >= pendingThreshold) {
                        transition = new Transition(AlertState.PENDING, AlertState.FIRING, windowStart,
                                "consecutive hits " + consecutiveHits + " reached pending threshold "
                                        + pendingThreshold);
                        state = AlertState.FIRING;
                        consecutiveHits = 0;
                        lastFiredWindowStart = windowStart;
                    }
                } else {
                    consecutiveHits = 0;
                }
            }
            case FIRING -> {
                if (hit) {
                    consecutiveMisses = 0;
                } else {
                    consecutiveHits = 0;
                    consecutiveMisses++;
                    if (consecutiveMisses >= resolveThreshold) {
                        transition = new Transition(AlertState.FIRING, AlertState.RESOLVED, windowStart,
                                "consecutive misses " + consecutiveMisses + " reached resolve threshold "
                                        + resolveThreshold);
                        state = AlertState.RESOLVED;
                        consecutiveMisses = 0;
                    }
                }
            }
        }
        return transition;
    }

    /** 一次状态迁移的不可变描述。 */
    public record Transition(AlertState from, AlertState to, long windowStart, String reason) {
    }
}
