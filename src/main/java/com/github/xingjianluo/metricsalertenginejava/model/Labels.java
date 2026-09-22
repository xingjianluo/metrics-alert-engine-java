package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * 不可变标签集合，按 key 排序以得到确定的时间线标识。
 * <p>标签名规则：{@code [A-Za-z_][A-Za-z0-9_.]*}；标签值非空、长度不超过 256 且不含控制字符。
 * 非法输入抛 {@link IllegalArgumentException}，由写入入口转换为 INVALID_LABEL 拒绝。
 */
public final class Labels {

    /** 合法标签名。 */
    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");
    private static final int MAX_VALUE_LENGTH = 256;

    private final TreeMap<String, String> values;

    private Labels(TreeMap<String, String> values) {
        this.values = values;
    }

    /** 用原始标签映射构造，null / 空 key 等非法标签在此抛出 {@link IllegalArgumentException}。 */
    public static Labels of(Map<String, String> raw) {
        if (raw == null) {
            throw new IllegalArgumentException("labels must not be null");
        }
        TreeMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<String, String> e : raw.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            if (key == null || !KEY_PATTERN.matcher(key).matches()) {
                throw new IllegalArgumentException("invalid label key: " + key);
            }
            if (value == null || value.isEmpty() || value.length() > MAX_VALUE_LENGTH) {
                throw new IllegalArgumentException("invalid label value for key " + key + ": " + value);
            }
            for (int i = 0; i < value.length(); i++) {
                if (Character.isISOControl(value.charAt(i))) {
                    throw new IllegalArgumentException("label value contains control character for key " + key);
                }
            }
            sorted.put(key, value);
        }
        return new Labels(sorted);
    }

    public static Labels empty() {
        return new Labels(new TreeMap<>());
    }

    public Map<String, String> asMap() {
        return java.util.Collections.unmodifiableMap(values);
    }

    public String get(String key) {
        return values.get(key);
    }

    public int size() {
        return values.size();
    }

    /** 形如 {host="a",service="x"} 的确定顺序表示，用作时间线 key 的一部分。 */
    public String canonicalString() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : values.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(e.getKey()).append('=').append('"').append(e.getValue()).append('"');
        }
        return sb.append('}').toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Labels other)) {
            return false;
        }
        return values.equals(other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(values);
    }

    @Override
    public String toString() {
        return canonicalString();
    }
}
