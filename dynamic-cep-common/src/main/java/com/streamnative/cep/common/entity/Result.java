package com.streamnative.cep.common.entity;

import java.math.BigDecimal;

public class Result {
    private Integer ruleId;
    private BigDecimal value;
    private Rule rule;

    public Result(Integer ruleId, BigDecimal value, Rule rule) {
        this.ruleId = ruleId;
        this.value = value;
        this.rule = rule;
    }

    public Result() {
    }

    public Integer getRuleId() {
        return ruleId;
    }

    public void setRuleId(Integer ruleId) {
        this.ruleId = ruleId;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public Rule getRule() {
        return rule;
    }

    public void setRule(Rule rule) {
        this.rule = rule;
    }
}
