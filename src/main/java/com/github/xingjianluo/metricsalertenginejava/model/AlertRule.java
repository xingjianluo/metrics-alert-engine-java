package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.Map;

/**
 * 不可变告警规则（一个版本一条记录）。
 *
 * @param version           规则版本
 * @param metric            监控的指标名
 * @param labelSelectors    标签过滤：key 匹配的标签值必须相等（空 map 表示不过滤）
 * @param windowMillis      聚合窗口大小（窗口按 epoch 对齐到该大小）
 * @param function          聚合算子
 * @param operator          阈值比较算子
 * @param threshold         阈值
 * @param forThreshold      连续满足多少次窗口评估后进入 FIRING
 * @param recoveryThreshold 连续不满足多少次窗口评估后回到 NORMAL
 * @param silenceMillis     静默期：同一规则+标签组合在该时间内不重复通知，但不影响状态记录
 */
public record AlertRule(
        RuleVersion version,
        String metric,
        Map<String, String> labelSelectors,
        long windowMillis,
        AggregateFunction function,
        ComparisonOperator operator,
        double threshold,
        int forThreshold,
        int recoveryThreshold,
        long silenceMillis) {

    public AlertRule {
        if (version == null) {
            throw new IllegalArgumentException("version is required");
        }
        if (metric == null || metric.isBlank()) {
            throw new IllegalArgumentException("metric is required");
        }
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be positive");
        }
        if (forThreshold < 1 || recoveryThreshold < 1) {
            throw new IllegalArgumentException("thresholds must be >= 1");
        }
        labelSelectors = labelSelectors == null ? Map.of() : Map.copyOf(labelSelectors);
    }

    /** 标签组合是否命中本规则：选择器中的每个 key 都必须等值匹配，未列出的标签不做限制。 */
    public boolean matches(Labels labels) {
        for (Map.Entry<String, String> sel : labelSelectors.entrySet()) {
            if (!sel.getValue().equals(labels.get(sel.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
