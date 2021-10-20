package com.streamnative.cep.common.acc;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

public class MaxAccumulator implements SimpleAccumulator<BigDecimal> {

    private BigDecimal max = BigDecimal.valueOf(Long.MIN_VALUE);

    public MaxAccumulator() {
    }

    public MaxAccumulator(BigDecimal value) {
        this.max = value;
    }

    @Override
    public void add(BigDecimal value) {
        this.max = max.max(value);
    }

    @Override
    public BigDecimal getLocalValue() {
        return max;
    }

    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        this.max = max.max(other.getLocalValue());
    }

    @Override
    public void resetLocal() {
        this.max = BigDecimal.valueOf(Long.MIN_VALUE);
    }

    @Override
    public MaxAccumulator clone() {
        MaxAccumulator clone = new MaxAccumulator();
        clone.max = max;
        return clone;
    }

}
