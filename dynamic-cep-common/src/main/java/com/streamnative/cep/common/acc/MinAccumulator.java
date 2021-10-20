package com.streamnative.cep.common.acc;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

public class MinAccumulator implements SimpleAccumulator<BigDecimal> {


    private BigDecimal min = BigDecimal.valueOf(Long.MAX_VALUE);


    public MinAccumulator() {
    }

    public MinAccumulator(BigDecimal value) {
        this.min = value;
    }

    @Override
    public void add(BigDecimal value) {
        this.min = min.min(value);
    }

    @Override
    public BigDecimal getLocalValue() {
        return this.min;
    }

    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        this.min = min.min(other.getLocalValue());
    }

    @Override
    public void resetLocal() {
        this.min = BigDecimal.valueOf(Long.MAX_VALUE);
    }

    @Override
    public MinAccumulator clone() {
        MinAccumulator clone = new MinAccumulator();
        clone.min = min;
        return clone;
    }

}
