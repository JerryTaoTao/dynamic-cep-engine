package com.streamnative.cep.common.sources;

import com.streamnative.cep.common.deserialize.MetricDeserialization;
import com.streamnative.cep.common.deserialize.RuleDeserialization;
import com.streamnative.cep.common.entity.Metric;
import com.streamnative.cep.common.entity.Rule;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.streamnative.cep.common.CommonDescriptor.*;

public class SourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SourceFactory.class);

    public static KafkaSource<?> factory(Map<String, String> props) {
        String sourceTag = props.getOrDefault(SOURCE_TAG, SOURCE_RULE_TAG);
        String bootstrapServers = props.get(KAFKA_BOOTSTRAP_SERVER);
        String topic = null;

        switch (sourceTag) {
            case SOURCE_RULE_TAG:
                topic = props.get(RULES_TOPIC);
                return KafkaSource.<Rule>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setGroupId("comsumer-rule-group")
                        .setTopics(topic)
                        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new RuleDeserialization()))
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setUnbounded(OffsetsInitializer.latest())
                        .build();
            case SOURCE_METRIC_TAG:
                topic = props.get(METRIC_TOPIC);
                return KafkaSource.<Metric>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setGroupId("comsumer-metric-group")
                        .setTopics(topic)
                        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new MetricDeserialization()))
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setUnbounded(OffsetsInitializer.latest())
                        .build();
            default:
                throw new RuntimeException("unsupport source tag " + sourceTag);
        }
    }
}
