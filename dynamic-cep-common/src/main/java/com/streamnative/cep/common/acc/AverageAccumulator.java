package com.streamnative.cep.common.acc;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

public class AverageAccumulator implements SimpleAccumulator<BigDecimal> {

  private static final long serialVersionUID = 1L;

  private BigDecimal count;

  private BigDecimal sum;

  @Override
  public void add(BigDecimal value) {
    this.count.add(BigDecimal.ONE);
    this.sum = sum.add(value);
  }

  @Override
  public BigDecimal getLocalValue() {
    if (count.equals(BigDecimal.ZERO)) {
      return count;
    }
    return this.sum.divide(count);
  }

  @Override
  public void resetLocal() {
    this.count = BigDecimal.ZERO;
    this.sum = BigDecimal.ZERO;
  }

  @Override
  public void merge(Accumulator<BigDecimal, BigDecimal> other) {
    if (other instanceof AverageAccumulator) {
      AverageAccumulator avg = (AverageAccumulator) other;
      this.count.add(avg.count);
      this.sum = sum.add(avg.sum);
    } else {
      throw new RuntimeException("AverageAccumulator merge failed with ." + other);
    }
  }

  @Override
  public AverageAccumulator clone() {
    AverageAccumulator clone = new AverageAccumulator();
    clone.count = this.count;
    clone.sum = this.sum;
    return clone;
  }

  @Override
  public String toString() {
    return String.format("AverageAccumulator: sum = %s,count = %s", sum, count);
  }
}
