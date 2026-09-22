package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 单次写入的结论。accepted=false 时 reason 给出可区分的拒绝原因；
 * duplicate=true 表示该时间戳样本已存在（按幂等覆盖处理，仍计 accepted）。
 */
public record WriteResult(boolean accepted, boolean duplicate, RejectReason reason, String message,
                          int metricVersion) {

    public static WriteResult ok(int metricVersion) {
        return new WriteResult(true, false, null, "accepted", metricVersion);
    }

    public static WriteResult okDuplicate(int metricVersion) {
        return new WriteResult(true, true, null, "duplicate timestamp overwritten", metricVersion);
    }

    public static WriteResult rejected(RejectReason reason, String message, int metricVersion) {
        return new WriteResult(false, false, reason, message, metricVersion);
    }
}
