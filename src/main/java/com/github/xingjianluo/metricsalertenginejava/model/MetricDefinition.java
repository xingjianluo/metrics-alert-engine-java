package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * 指标定义（一个版本一条不可变记录）：描述一个指标的语义口径。
 *
 * @param version            该定义所属的指标与版本号
 * @param unit               计量单位（如 "ms"、"%"），null 表示未声明
 * @param description        指标口径说明，可为 null
 * @param defaultAggregation 默认聚合口径；查询不指定算子时按它解读窗口
 * @param minValue           允许的取值下界（含），null 表示不限下界
 * @param maxValue           允许的取值上界（含），null 表示不限上界
 * @param allowedLabelKeys   允许出现的标签键白名单；<b>空集表示不限制</b>（未登记定义的宽松语义）
 * @param createdAtMillis    该版本创建时间（epoch millis）
 */
public record MetricDefinition(
        MetricVersion version,
        String unit,
        String description,
        AggregateFunction defaultAggregation,
        Double minValue,
        Double maxValue,
        Set<String> allowedLabelKeys,
        long createdAtMillis) {

    /** 与 {@link Labels} 的标签名规则保持一致。 */
    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    public MetricDefinition {
        if (version == null) {
            throw new IllegalArgumentException("version is required");
        }
        if (defaultAggregation == null) {
            throw new IllegalArgumentException("defaultAggregation is required");
        }
        if (unit != null && unit.isBlank()) {
            throw new IllegalArgumentException("unit must not be blank; use null to omit");
        }
        if (minValue != null && maxValue != null && minValue > maxValue) {
            throw new IllegalArgumentException("minValue " + minValue + " must not exceed maxValue " + maxValue);
        }
        if (allowedLabelKeys == null) {
            allowedLabelKeys = Set.of();
        } else {
            for (String key : allowedLabelKeys) {
                if (key == null || !KEY_PATTERN.matcher(key).matches()) {
                    throw new IllegalArgumentException("invalid allowed label key: " + key);
                }
            }
            allowedLabelKeys = Set.copyOf(allowedLabelKeys);
        }
    }

    /** 创建 / 更新定义时由调用方提供的语义信息（版本号与时间戳由服务端分配）。 */
    public record Spec(String unit,
                       String description,
                       AggregateFunction defaultAggregation,
                       Double minValue,
                       Double maxValue,
                       Set<String> allowedLabelKeys) {

        public Spec {
            if (defaultAggregation == null) {
                throw new IllegalArgumentException("defaultAggregation is required");
            }
            if (unit != null && unit.isBlank()) {
                throw new IllegalArgumentException("unit must not be blank; use null to omit");
            }
            if (minValue != null && maxValue != null && minValue > maxValue) {
                throw new IllegalArgumentException("minValue " + minValue + " must not exceed maxValue " + maxValue);
            }
            if (allowedLabelKeys != null) {
                for (String key : allowedLabelKeys) {
                    if (key == null || !KEY_PATTERN.matcher(key).matches()) {
                        throw new IllegalArgumentException("invalid allowed label key: " + key);
                    }
                }
                allowedLabelKeys = Set.copyOf(allowedLabelKeys);
            }
        }
    }

    /** 是否声明了取值范围（任一侧受限即需要按范围校验）。 */
    public boolean hasValueRange() {
        return minValue != null || maxValue != null;
    }

    /**
     * 值是否落在声明范围内；未声明范围时一切值都合法。
     * NaN / 无穷大一律视为越界（采样值必须是有限数）。
     */
    public boolean isValueInRange(double value) {
        if (!Double.isFinite(value)) {
            return false;
        }
        if (minValue != null && value < minValue) {
            return false;
        }
        return maxValue == null || value <= maxValue;
    }

    /** 人类可读的范围描述，用于拒绝明细。 */
    public String rangeDescription() {
        if (minValue != null && maxValue != null) {
            return "[" + minValue + "," + maxValue + "]";
        }
        if (minValue != null) {
            return "[" + minValue + ",+inf)";
        }
        if (maxValue != null) {
            return "(-inf," + maxValue + "]";
        }
        return "(-inf,+inf)";
    }

    /**
     * 白名单是否启用：空集表示“不限制标签键”，与未登记定义的宽松行为一致；
     * 非空时只允许集合内的键出现。
     */
    public boolean restrictsLabelKeys() {
        return !allowedLabelKeys.isEmpty();
    }

    /** 返回标签集合中不在白名单内的键；白名单未启用时永远返回空集。 */
    public java.util.List<String> disallowedKeys(Labels labels) {
        if (!restrictsLabelKeys()) {
            return java.util.List.of();
        }
        java.util.List<String> out = new java.util.ArrayList<>();
        for (String key : labels.asMap().keySet()) {
            if (!allowedLabelKeys.contains(key)) {
                out.add(key);
            }
        }
        return out;
    }
}
