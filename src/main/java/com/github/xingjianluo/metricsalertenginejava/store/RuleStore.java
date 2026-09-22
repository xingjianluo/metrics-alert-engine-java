package com.github.xingjianluo.metricsalertenginejava.store;

import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.model.ComparisonOperator;
import com.github.xingjianluo.metricsalertenginejava.model.RuleVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 规则版本库：同一 ruleId 的每次更新产生单调递增的新版本，旧版本不可变并永久保留，
 * 因此针对任意历史版本的求值结论都可以按版本复现。
 */
public class RuleStore {

    /** 不含版本号的规则定义，用于创建 / 更新。 */
    public record RuleSpec(String metric,
                           Map<String, String> labelSelectors,
                           long windowMillis,
                           AggregateFunction function,
                           ComparisonOperator operator,
                           double threshold,
                           int forThreshold,
                           int recoveryThreshold,
                           long silenceMillis) {
    }

    /** ruleId -> 按版本号有序的全部版本 */
    private final ConcurrentHashMap<String, NavigableMap<Integer, AlertRule>> rules = new ConcurrentHashMap<>();

    public AlertRule register(String ruleId, RuleSpec spec) {
        NavigableMap<Integer, AlertRule> versions =
                rules.computeIfAbsent(ruleId, ignored -> new ConcurrentSkipListMap<>());
        synchronized (versions) {
            if (!versions.isEmpty()) {
                throw new IllegalStateException("rule already exists: " + ruleId
                        + "; use update() to create a new version");
            }
            AlertRule rule = toRule(new RuleVersion(ruleId, 1), spec);
            versions.put(1, rule);
            return rule;
        }
    }

    public AlertRule update(String ruleId, RuleSpec spec) {
        NavigableMap<Integer, AlertRule> versions = rules.get(ruleId);
        if (versions == null) {
            throw new IllegalArgumentException("rule not found: " + ruleId);
        }
        synchronized (versions) {
            int nextVersion = versions.lastKey() + 1;
            AlertRule rule = toRule(new RuleVersion(ruleId, nextVersion), spec);
            versions.put(nextVersion, rule);
            return rule;
        }
    }

    private static AlertRule toRule(RuleVersion version, RuleSpec spec) {
        return new AlertRule(version, spec.metric(), spec.labelSelectors(), spec.windowMillis(),
                spec.function(), spec.operator(), spec.threshold(), spec.forThreshold(),
                spec.recoveryThreshold(), spec.silenceMillis());
    }

    public AlertRule latest(String ruleId) {
        NavigableMap<Integer, AlertRule> versions = rules.get(ruleId);
        if (versions == null || versions.isEmpty()) {
            throw new IllegalArgumentException("rule not found: " + ruleId);
        }
        return versions.lastEntry().getValue();
    }

    public AlertRule get(RuleVersion version) {
        NavigableMap<Integer, AlertRule> versions = rules.get(version.ruleId());
        AlertRule rule = versions == null ? null : versions.get(version.version());
        if (rule == null) {
            throw new IllegalArgumentException("rule version not found: " + version);
        }
        return rule;
    }

    public List<AlertRule> versions(String ruleId) {
        NavigableMap<Integer, AlertRule> versions = rules.get(ruleId);
        return versions == null ? List.of() : new ArrayList<>(versions.values());
    }

    public List<AlertRule> allLatest() {
        List<AlertRule> latest = new ArrayList<>();
        rules.values().forEach(versions -> {
            if (!versions.isEmpty()) {
                latest.add(versions.lastEntry().getValue());
            }
        });
        latest.sort((a, b) -> a.version().compareTo(b.version()));
        return latest;
    }
}
