package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * 不可变、有序的标签组合。同一指标下不同 LabelSet 的时间序列相互隔离。
 * 校验规则：key/value 非空、不含保留分隔符；key 至少 1 个字符。
 */
public final class LabelSet {

    /** 序列标识的保留分隔符，标签 key/value 中禁止出现。 */
    public static final String SEPARATOR = "|";

    private final Map<String, String> labels;

    public LabelSet(Map<String, String> labels) {
        this.labels = Collections.unmodifiableMap(new TreeMap<>(labels == null ? Map.of() : labels));
    }

    public static LabelSet of(Map<String, String> labels) {
        return new LabelSet(labels);
    }

    public static LabelSet empty() {
        return new LabelSet(Map.of());
    }

    public Map<String, String> labels() {
        return labels;
    }

    public int size() {
        return labels.size();
    }

    /** 校验标签合法性，非法时抛出 IllegalArgumentException（消息可区分原因）。 */
    public void validate() {
        // TODO 骨架
    }

    /** 确定性的序列标识：按 key 排序后拼接。 */
    public String seriesKey() {
        // TODO 骨架
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LabelSet labelSet)) {
            return false;
        }
        return Objects.equals(labels, labelSet.labels);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(labels);
    }

    @Override
    public String toString() {
        return labels.toString();
    }
}
