package com.streamnative.cep.common;

public interface CommonDescriptor {
    String PARTITION_FIELDS_MAP_NAME = "tags";
    String RULES_TOPIC = "rule-topics";
    String METRIC_TOPIC = "metric-topics";
    String SOURCE_TAG = "source-tag";
    String SOURCE_RULE_TAG = "rule";
    String SOURCE_METRIC_TAG = "metric";
    String KAFKA_BOOTSTRAP_SERVER = "bootstrap-servers";
}
