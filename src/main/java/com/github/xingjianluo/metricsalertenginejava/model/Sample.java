package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 一条指标采样：指标名 + 标签组合 + 值 + 毫秒时间戳。
 *
 * @param metric          指标名
 * @param labels          标签组合（不可变）
 * @param value           采样值
 * @param timestampMillis 采样时间戳（epoch millis）
 */
public record Sample(String metric, Labels labels, double value, long timestampMillis) {
}
