package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 规则版本标识：同一条规则（ruleId）每次更新产生新版本，历史结论按版本复现。
 */
public record RuleVersion(String ruleId, int version) implements Comparable<RuleVersion> {

    public RuleVersion {
        if (ruleId == null || ruleId.isBlank()) {
            throw new IllegalArgumentException("ruleId must not be blank");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
    }

    public RuleVersion next() {
        return new RuleVersion(ruleId, version + 1);
    }

    @Override
    public int compareTo(RuleVersion o) {
        int c = ruleId.compareTo(o.ruleId);
        return c != 0 ? c : Integer.compare(version, o.version);
    }

    @Override
    public String toString() {
        return ruleId + "#v" + version;
    }
}
