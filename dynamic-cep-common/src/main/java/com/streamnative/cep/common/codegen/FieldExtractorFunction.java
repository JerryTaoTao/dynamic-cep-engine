package com.streamnative.cep.common.codegen;

import com.streamnative.cep.common.entity.Metric;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.streamnative.cep.common.CommonDescriptor.PARTITION_FIELDS_MAP_NAME;

/**
 *
 */
public abstract class FieldExtractorFunction {

    private Integer ruleId;
    private List<String> partitionKeys;
    private String partitionMapName = PARTITION_FIELDS_MAP_NAME;

    public FieldExtractorFunction(Integer ruleId, List<String> partitionKeys, String partitionMapName) {
        this.ruleId = ruleId;
        this.partitionKeys = partitionKeys;
        this.partitionMapName = partitionMapName;
    }

    public FieldExtractorFunction(Integer ruleId, List<String> partitionKeys) {
        this.ruleId = ruleId;
        this.partitionKeys = partitionKeys;
    }

    public FieldExtractorFunction(Integer ruleId) {
        this.ruleId = ruleId;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public String getPartitionMapName() {
        return partitionMapName;
    }

    public void setPartitionMapName(String partitionMapName) {
        this.partitionMapName = partitionMapName;
    }

    public List<Tuple2<String, String>> getPartitionFieldList(List<String> partitionKeys, Metric input) {
        Map<String, String> partitionFieldMap = input.getTags();
        return partitionKeys.stream().map(partitionKey -> {
            String value = partitionFieldMap.get(partitionKey);
            return Tuple2.of(partitionKey, value);
        }).collect(Collectors.toList());
    }

    public List<Tuple2<String, String>> getPartitionFieldList(Metric input) {
        return getPartitionFieldList(this.partitionKeys, input);
    }

    public FieldExtractorFunction() {
    }

    public Integer getRuleId() {
        return ruleId;
    }

    public void setRuleId(Integer ruleId) {
        this.ruleId = ruleId;
    }
}
