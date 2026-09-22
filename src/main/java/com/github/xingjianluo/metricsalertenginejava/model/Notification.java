package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 一次通知结论。仅在进入 FIRING（首次触发）与回到 NORMAL（恢复完成）时产生。
 */
public record Notification(
        RuleVersion ruleVersion,
        String metric,
        Labels labels,
        Type type,
        long timestampMillis,
        boolean suppressed,
        String detail) {

    public enum Type { FIRING, RECOVERED }
}
