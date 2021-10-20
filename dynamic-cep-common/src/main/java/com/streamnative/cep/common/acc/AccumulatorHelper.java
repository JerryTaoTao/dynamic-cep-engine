package com.streamnative.cep.common.acc;

import com.streamnative.cep.common.entity.Rule;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

public interface AccumulatorHelper {

    default SimpleAccumulator<BigDecimal> getAccumulator(Rule.AggregatorFunctionType aggregatorFunctionType) {
        switch (aggregatorFunctionType) {
            case SUM:
                return new CounterAccumulator();
            case AVG:
                return new AverageAccumulator();
            case MAX:
                return new MaxAccumulator();
            case MIN:
                return new MinAccumulator();
            default:
                throw new RuntimeException(
                        "Unsupported aggregation function type: " + aggregatorFunctionType);
        }
    }
}
