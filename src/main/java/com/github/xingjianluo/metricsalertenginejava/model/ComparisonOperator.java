package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 阈值比较算子。
 */
public enum ComparisonOperator {
    GT, GE, LT, LE, EQ;

    public boolean test(double actual, double threshold) {
        return switch (this) {
            case GT -> actual > threshold;
            case GE -> actual >= threshold;
            case LT -> actual < threshold;
            case LE -> actual <= threshold;
            case EQ -> Double.compare(actual, threshold) == 0;
        };
    }
}
