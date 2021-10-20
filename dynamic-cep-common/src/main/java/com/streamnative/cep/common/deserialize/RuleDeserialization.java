package com.streamnative.cep.common.deserialize;

import com.alibaba.fastjson.JSON;
import com.streamnative.cep.common.entity.Rule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RuleDeserialization implements DeserializationSchema<Rule> {

    private static final Logger LOG = LoggerFactory.getLogger(RuleDeserialization.class);

    @Override
    public Rule deserialize(byte[] message) throws IOException {
        String msg = new String(message);
        Rule rule = null;
        try {
            rule = JSON.parseObject(msg, Rule.class);
        } catch (Exception e) {
            //todo add metric for parse failed counter.
            LOG.error("deserialize the input {} to Rule failed. ", msg, e);
        }
        return rule;
    }

    @Override
    public boolean isEndOfStream(Rule nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Rule> getProducedType() {
        TypeInformation<Rule> type = TypeExtractor.getForClass(Rule.class);
        return type;
    }
}
