package com.streamnative.cep.common;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * interface for access field from pojo object.
 *
 * @param <IN> the input object type.
 * @param <O>  the output type type.
 */
public interface DynamicFieldAccessor<IN, O> extends Serializable {
    /**
     * @param field dynamic partition key.
     * @param obj   the pojo object
     * @return a Tuple2 list with field and value.
     */
    O getPartitionField(String field,
                        IN obj) throws IllegalAccessException, NoSuchFieldException;

    /**
     * @param partitionKeys dynamic partition keys.
     * @param input         the input to generate partition key and value pairs.
     * @return a list with tuple2 type.
     */
    List<Tuple2<String, String>> getPartitionFieldList(List<String> partitionKeys, IN input) throws NoSuchFieldException, IllegalAccessException;
}
