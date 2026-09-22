package com.github.xingjianluo.metricsalertenginejava.rule;

import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;

/**
 * 单个窗口的求值记录（结论可解释）：输入值、是否命中、判定前后状态、迁移原因。
 */
public record EvaluationRecord(String ruleId, int ruleVersion, LabelSet labels, long windowStart,
                               Double inputValue, double threshold, boolean hit, AlertState stateBefore,
                               AlertState stateAfter, String transitionReason) {

    public String explain() {
        // TODO 骨架
        return null;
    }
}
