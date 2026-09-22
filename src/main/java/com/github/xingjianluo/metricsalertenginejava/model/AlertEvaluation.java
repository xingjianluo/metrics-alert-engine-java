package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 一条规则 × 一个标签组合在某个窗口上的求值结论（状态机的一次迁移记录）。
 */
public record AlertEvaluation(
        RuleVersion ruleVersion,
        String metric,
        Labels labels,
        long windowStart,
        double aggregatedValue,
        boolean conditionMet,
        AlertState fromState,
        AlertState toState,
        int consecutiveHits,
        int consecutiveMisses,
        String reason) {
}
