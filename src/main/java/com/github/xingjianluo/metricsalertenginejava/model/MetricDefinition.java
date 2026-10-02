package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.Set;

/**
 * 指标定义（不可变，按版本留存）：描述一个指标的语义口径。
 * 每次更新产生单调递增的新版本，旧版本永不覆盖，可按版本复现历史解读。
 *
 * @param metric           指标名
 * @param version          定义版本号（从 1 开始单调递增）
 * @param unit             单位（如 "ms"、"percent"），可空
 * @param description      说明，可空
 * @param defaultFunction  默认聚合口径（查询未显式指定 function 时使用）
 * @param minValue         取值下界（含），null 表示不限制
 * @param maxValue         取值上界（含），null 表示不限制
 * @param allowedLabelKeys 允许出现的标签键；空集合表示不限制
 * @param createdAtMillis  该版本登记时间
 */
public record MetricDefinition(
        String metric,
        int version,
        String unit,
        String description,
        AggregateFunction defaultFunction,
        Double minValue,
        Double maxValue,
        Set<String> allowedLabelKeys,
        long createdAtMillis) {

    /** 值是否落在本版本定义的取值范围内（未定义范围则一律合法）。 */
    public boolean valueInRange(double value) {
        if (minValue != null && value < minValue) {
            return false;
        }
        return maxValue == null || value <= maxValue;
    }

    /** 标签键是否被本版本定义允许（未定义允许集则一律合法）。 */
    public boolean labelKeyAllowed(String key) {
        return allowedLabelKeys == null || allowedLabelKeys.isEmpty() || allowedLabelKeys.contains(key);
    }
}
