package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.Aggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 规则的版本化注册表。规则更新产生新版本；历史版本保留，其求值记录由 AlertEvaluator
 * 按 (规则版本, 标签) 独立保存，支持规则变更后按版本复现历史结论。并发安全。
 */
public final class RuleRegistry {

    private final Map<String, NavigableMap<Integer, AlertRule>> versions = new ConcurrentHashMap<>();
    private final int maxRules;

    public RuleRegistry(int maxRules) {
        this.maxRules = maxRules;
    }

    /** 注册新规则，规则自身的 version 字段必须为 1；重名/超容量抛出 IllegalArgumentException。 */
    public synchronized AlertRule register(AlertRule rule) {
        if (rule.version() != 1) {
            throw new IllegalArgumentException(
                    "rule.version.invalid: new rule must start at version 1: " + rule.id());
        }
        if (versions.containsKey(rule.id())) {
            throw new IllegalArgumentException("rule.already.exists: rule " + rule.id() + " already registered");
        }
        if (versions.size() >= maxRules) {
            throw new IllegalArgumentException(
                    "rule.capacity.exceeded: max rules " + maxRules + " reached, rejected " + rule.id());
        }
        NavigableMap<Integer, AlertRule> map = new ConcurrentSkipListMap<>();
        map.put(1, rule);
        versions.put(rule.id(), map);
        return rule;
    }

    /** 用给定规则体创建下一版本（版本号由注册表分配），metric/windowSize 沿用最新版本。 */
    public synchronized AlertRule update(String id, AlertRule.Operator operator, Aggregator aggregator,
                                         double threshold, int pendingThreshold, int resolveThreshold,
                                         long silenceMillis, Set<String> labelFilter) {
        NavigableMap<Integer, AlertRule> map = versions.get(id);
        if (map == null) {
            throw new IllegalArgumentException("rule.unknown: cannot update unknown rule " + id);
        }
        AlertRule latest = map.lastEntry().getValue();
        int newVersion = latest.version() + 1;
        AlertRule next = new AlertRule(id, newVersion, latest.metric(), aggregator, operator, threshold,
                pendingThreshold, resolveThreshold, latest.windowSizeMillis(), silenceMillis, labelFilter);
        map.put(newVersion, next);
        return next;
    }

    public AlertRule current(String id) {
        NavigableMap<Integer, AlertRule> map = versions.get(id);
        return map == null ? null : map.lastEntry().getValue();
    }

    /** 按版本号取规则；version 为 null 时取当前版本；不存在返回 null。 */
    public AlertRule version(String id, Integer version) {
        NavigableMap<Integer, AlertRule> map = versions.get(id);
        if (map == null) {
            return null;
        }
        return version == null ? map.lastEntry().getValue() : map.get(version);
    }

    /** 全部历史版本，按版本号升序。 */
    public List<AlertRule> history(String id) {
        NavigableMap<Integer, AlertRule> map = versions.get(id);
        return map == null ? List.of() : new ArrayList<>(map.values());
    }

    /** 每个规则的当前版本。 */
    public List<AlertRule> allCurrent() {
        List<AlertRule> out = new ArrayList<>();
        for (NavigableMap<Integer, AlertRule> map : versions.values()) {
            out.add(map.lastEntry().getValue());
        }
        return List.copyOf(out);
    }
}
