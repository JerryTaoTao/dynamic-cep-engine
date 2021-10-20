package com.streamnative.cep.common.field.accessor;

import com.streamnative.cep.common.DynamicFieldAccessor;
import com.streamnative.cep.common.entity.Metric;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.streamnative.cep.common.CommonDescriptor.PARTITION_FIELDS_MAP_NAME;

/**
 * use reflect to get field value that will be lower speed
 */
public class ReflectFieldsAccessor implements DynamicFieldAccessor<Metric, Map<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(ReflectFieldsAccessor.class);
    private Class<?> clazz;

    public ReflectFieldsAccessor(Class<?> clazz) {
        this.clazz = clazz;
    }

    public ReflectFieldsAccessor() {
        this.clazz = Metric.class;
    }

    @Override
    public Map<String, String> getPartitionField(String filedName, Metric obj) throws IllegalAccessException, NoSuchFieldException {
        Field field = clazz.getDeclaredField(filedName);
        field.setAccessible(true);
        return (Map<String, String>) field.get(obj);
    }

    @Override
    public List<Tuple2<String, String>> getPartitionFieldList(List<String> partitionKeys, Metric input) throws NoSuchFieldException, IllegalAccessException {
        if (input == null) {
            LOG.warn("input is null. ");
            return Collections.emptyList();
        }
        Map<String, String> partitionFieldMap = getPartitionField(PARTITION_FIELDS_MAP_NAME, input);
        return partitionKeys.stream().map(partitionKey -> {
            String value = partitionFieldMap.get(partitionKey);
            return Tuple2.of(partitionKey, value);
        }).collect(Collectors.toList());
    }
}
