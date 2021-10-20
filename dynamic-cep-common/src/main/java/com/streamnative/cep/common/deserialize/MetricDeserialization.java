package com.streamnative.cep.common.deserialize;

import com.alibaba.fastjson.JSON;
import com.streamnative.cep.common.entity.Metric;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MetricDeserialization implements DeserializationSchema<Metric> {

    private static final Logger LOG = LoggerFactory.getLogger(MetricDeserialization.class);

    @Override
    public Metric deserialize(byte[] message) throws IOException {
        String msg = new String(message);
        Metric metric = null;
        try {
            metric = JSON.parseObject(msg, Metric.class);
        } catch (Exception e) {
            //todo add metric for parse failed counter.
            LOG.error("deserialize the input {} to Rule failed. ", msg, e);
        }
        return metric;
    }

    @Override
    public boolean isEndOfStream(Metric nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Metric> getProducedType() {
        TypeInformation<Metric> type = TypeExtractor.getForClass(Metric.class);
        return type;
    }
}
