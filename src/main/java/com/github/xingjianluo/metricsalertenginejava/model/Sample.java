package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 一条采集样本：指标名 + 标签组合 + 值 + 毫秒时间戳。
 * 同一序列 (metric, labels) 下相同时间戳的重复样本按最后写入覆盖（去重幂等）。
 */
public record Sample(String metric, LabelSet labels, double value, long timestampMillis) {

    public Sample {
        // 防御性拷贝，保证不可变
        labels = labels == null ? LabelSet.empty() : labels;
    }

    public static Sample of(String metric, LabelSet labels, double value, long timestampMillis) {
        return new Sample(metric, labels, value, timestampMillis);
    }
}
