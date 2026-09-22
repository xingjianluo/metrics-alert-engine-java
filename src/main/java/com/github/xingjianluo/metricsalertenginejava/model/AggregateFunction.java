package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 窗口聚合算子。
 */
public enum AggregateFunction {
    SUM, COUNT, AVG, MIN, MAX;

    public double apply(WindowStats stats) {
        return switch (this) {
            case SUM -> stats.sum();
            case COUNT -> stats.count();
            case AVG -> stats.avg();
            case MIN -> stats.min();
            case MAX -> stats.max();
        };
    }
}
