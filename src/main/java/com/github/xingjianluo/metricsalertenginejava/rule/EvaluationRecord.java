package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;

/**
 * 单个窗口的求值记录（结论可解释）：输入值、是否命中、判定前后状态、迁移原因。
 */
public record EvaluationRecord(String ruleId, int ruleVersion, LabelSet labels, long windowStart,
                               Double inputValue, double threshold, boolean hit, AlertState stateBefore,
                               AlertState stateAfter, String transitionReason) {

    /** 人类可读的判定依据：规则版本、窗口、输入值、阈值、是否命中、状态迁移。 */
    public String explain() {
        return "rule=" + ruleId + "@v" + ruleVersion + " labels=" + labels + " window=" + windowStart
                + " input=" + inputValue + " threshold=" + threshold + " hit=" + hit
                + " state " + stateBefore + "->" + stateAfter
                + (transitionReason == null ? " (no transition)" : " (" + transitionReason + ")");
    }
}
