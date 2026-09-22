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

    /**
     * 校验标签合法性，非法时抛出 IllegalArgumentException，消息前缀可区分原因：
     * label.key.empty / label.key.separator / label.value.null / label.value.separator。
     */
    public void validate() {
        for (Map.Entry<String, String> e : labels.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("label.key.empty: label key must not be null or empty");
            }
            if (key.contains(SEPARATOR)) {
                throw new IllegalArgumentException(
                        "label.key.separator: label key must not contain '" + SEPARATOR + "': " + key);
            }
            if (value == null) {
                throw new IllegalArgumentException(
                        "label.value.null: value for label key '" + key + "' must not be null");
            }
            if (value.contains(SEPARATOR)) {
                throw new IllegalArgumentException(
                        "label.value.separator: label value must not contain '" + SEPARATOR + "': " + key + "="
                                + value);
            }
        }
    }

    /** 确定性的序列标识：按 key 排序后拼接，形如 {@code |k1=v1|k2=v2|}，空标签为单个分隔符。 */
    public String seriesKey() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : labels.entrySet()) {
            sb.append(SEPARATOR).append(e.getKey()).append('=').append(e.getValue());
        }
        sb.append(SEPARATOR);
        return sb.toString();
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
