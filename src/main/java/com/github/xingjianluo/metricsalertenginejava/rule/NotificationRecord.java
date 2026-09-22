package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;

/**
 * 一条通知结论。suppressed=true 表示处于静默期被抑制（状态迁移仍然记录）。
 * dedupKey 用于同一规则+标签+状态的通知去重。
 */
public record NotificationRecord(String ruleId, int ruleVersion, LabelSet labels, AlertState notifyState,
                                 long windowStart, long timestampMillis, boolean suppressed, String dedupKey,
                                 String message) {
}
