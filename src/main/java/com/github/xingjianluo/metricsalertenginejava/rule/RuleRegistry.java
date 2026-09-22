package com.github.xingjianluo.metricsalertenginejava.rule;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 规则的版本化注册表。规则更新产生新版本；历史版本连同其求值记录保留，
 * 支持规则变更后按版本复现历史结论。
 */
public final class RuleRegistry {

    private final Map<String, NavigableMap<Integer, AlertRule>> versions = new ConcurrentHashMap<>();
    private final int maxRules;

    public RuleRegistry(int maxRules) {
        this.maxRules = maxRules;
    }

    public AlertRule register(AlertRule rule) {
        // TODO 骨架
        return null;
    }

    /** 用给定规则体创建下一版本（version 由注册表分配）。 */
    public AlertRule update(String id, AlertRule.Operator operator, com.github.xingjianluo.metricsalertenginejava.model.Aggregator aggregator,
                            double threshold, int pendingThreshold, int resolveThreshold, long silenceMillis,
                            java.util.Set<String> labelFilter) {
        // TODO 骨架
        return null;
    }

    public AlertRule current(String id) {
        // TODO 骨架
        return null;
    }

    public AlertRule version(String id, Integer version) {
        // TODO 骨架
        return null;
    }

    public List<AlertRule> history(String id) {
        // TODO 骨架
        return List.of();
    }

    public List<AlertRule> allCurrent() {
        // TODO 骨架
        return List.of();
    }
}
