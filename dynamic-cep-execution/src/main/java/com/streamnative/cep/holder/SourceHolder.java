package com.streamnative.cep.holder;

import com.streamnative.cep.ExecutionDescriptor;
import com.streamnative.cep.common.entity.Metric;
import com.streamnative.cep.common.entity.Rule;
import com.streamnative.cep.common.function.MetricTimestampExtractor;
import com.streamnative.cep.common.sources.SourceFactory;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;
import java.util.Objects;

import static com.streamnative.cep.common.CommonDescriptor.SOURCE_METRIC_TAG;
import static com.streamnative.cep.common.CommonDescriptor.SOURCE_RULE_TAG;

/**
 * a source holder for control to generate source
 */
public interface SourceHolder {

    default DataStream<Rule> getRuleSource(Map<String, String> props, StreamExecutionEnvironment executionEnvironment) {
        Integer parallelism = Integer.valueOf(props.getOrDefault(ExecutionDescriptor.SOURCE_RULE_PARALLELISM, "1"));
        KafkaSource<Rule> ruleKafkaSource = (KafkaSource<Rule>) SourceFactory.factory(props);
        DataStream<Rule> ruleDataStream = executionEnvironment
                .fromSource(ruleKafkaSource, null, "rule-source")
                .filter(Objects::nonNull)
                .setParallelism(parallelism);
        return ruleDataStream;

    }

    default DataStream<?> getSource(Map<String, String> props, StreamExecutionEnvironment executionEnvironment, String sourceTag) {
        switch (sourceTag) {
            case SOURCE_RULE_TAG:
                return getRuleSource(props, executionEnvironment);
            case SOURCE_METRIC_TAG:
                return getMetricSource(props, executionEnvironment);
            default:
                throw new RuntimeException("unknow source tag" + sourceTag);
        }
    }

    default DataStream<Metric> getMetricSource(Map<String, String> props, StreamExecutionEnvironment executionEnvironment) {
        Integer parallelism = executionEnvironment.getParallelism();
        if (props.containsKey(ExecutionDescriptor.SOURCE_METRIC_PARALLELISM)) {
            parallelism = Integer.valueOf(props.get(ExecutionDescriptor.SOURCE_METRIC_PARALLELISM));
        }

        KafkaSource<Metric> ruleKafkaSource = (KafkaSource<Metric>) SourceFactory.factory(props);
        DataStream<Metric> metricDataStream = executionEnvironment
                .fromSource(ruleKafkaSource, null, "rule-source")
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(new MetricTimestampExtractor(Time.minutes(5L)))
                .setParallelism(parallelism);
        return metricDataStream;
    }
}
