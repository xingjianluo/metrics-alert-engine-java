package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 单次写入的结论。accepted=false 时 reason 给出可区分的拒绝原因。
 */
public record WriteResult(boolean accepted, RejectReason reason, String detail, Sample sample) {

    public static WriteResult accepted(Sample sample) {
        return new WriteResult(true, null, "accepted", sample);
    }

    public static WriteResult rejected(RejectReason reason, String detail, Sample sample) {
        return new WriteResult(false, reason, detail, sample);
    }
}
