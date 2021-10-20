package com.streamnative.cep.common.function;

import com.streamnative.cep.common.entity.Metric;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MetricTimestampExtractor
        extends BoundedOutOfOrdernessTimestampExtractor<Metric> {
    public MetricTimestampExtractor(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Metric element) {
        return element.getEventTime();
    }


}
