package com.streamnative.cep.common.acc;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

public class CounterAccumulator implements SimpleAccumulator<BigDecimal> {

    private BigDecimal localValue = BigDecimal.ZERO;

    public CounterAccumulator() {
    }

    public CounterAccumulator(BigDecimal value) {
        this.localValue = value;
    }

    @Override
    public void add(BigDecimal value) {
        localValue = localValue.add(value);
    }

    @Override
    public BigDecimal getLocalValue() {
        return localValue;
    }

    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        localValue = localValue.add(other.getLocalValue());
    }

    @Override
    public void resetLocal() {
        this.localValue = BigDecimal.ZERO;
    }

    @Override
    public CounterAccumulator clone() {
        CounterAccumulator clone = new CounterAccumulator();
        clone.localValue = localValue;
        return clone;
    }

}
