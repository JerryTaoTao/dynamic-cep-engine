package com.streamnative.cep;

import com.streamnative.cep.common.entity.Metric;
import com.streamnative.cep.common.entity.Rule;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;

/**
 *
 */
public interface ExecutionDescriptor {
    String TIMECHARACTERISTIC = "timecharacteristic";
    String TIMECHARACTERISTIC_PROCTIME = "proc-time";
    String TIMECHARACTERISTIC_EVENT_TIME = "event-time";
    String TIMECHARACTERISTIC_INGESTION_TIME = "ingestion-time";
    String WATERMARK_INTERVAL = "watermark.interval";
    String SOURCE_RULE_PARALLELISM = "source.rule.parallelism";
    String SOURCE_METRIC_PARALLELISM = "source.mertic.parallelism";
    long WATERMARK_INTERVAL_DEFAULT = 1000;
    String CHECKPOINT_INTERVAL = "checkpoint.interval";
    long CHECKPOINT_INTERVAL_DEFAULT = 300000;
    String CHECKPOINT_TIMEOUT = "checkpoint.timeout";
    long CHECKPOINT_TIMEOUT_DEFAULT = 300000;
    MapStateDescriptor<Integer, Rule> rulesDescriptor =
            new MapStateDescriptor<>(
                    "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));

    MapStateDescriptor<Long, List<Metric>> windowStateDescriptor =
            new MapStateDescriptor<Long, List<Metric>>(
                    "buffer-window-state",
                    BasicTypeInfo.LONG_TYPE_INFO,
                    TypeInformation.of(new TypeHint<List<Metric>>() {
                    }));
}
