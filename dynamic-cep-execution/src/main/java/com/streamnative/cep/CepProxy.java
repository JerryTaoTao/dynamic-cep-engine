package com.streamnative.cep;

import com.streamnative.cep.common.entity.Metric;
import com.streamnative.cep.common.entity.Result;
import com.streamnative.cep.common.entity.Rule;
import com.streamnative.cep.functions.DynamicPartitionProcessFunction;
import com.streamnative.cep.functions.DynamicApplyRuleProcessFunction;
import com.streamnative.cep.holder.EnvironmentHolder;
import com.streamnative.cep.holder.SourceHolder;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.streamnative.cep.common.CommonDescriptor.SOURCE_METRIC_TAG;
import static com.streamnative.cep.common.CommonDescriptor.SOURCE_RULE_TAG;

public class CepProxy implements EnvironmentHolder, SourceHolder {

    private static final Logger LOG = LoggerFactory.getLogger(CepProxy.class);

    public CepProxy() {
    }

    public void execute(Map<String, String> props) throws Exception {
        LOG.info("cep proxy start successful.");
        // create  Environment
        StreamExecutionEnvironment env = createExecutionEnv(props);

        // create rule and metric source datastream
        DataStream<Rule> ruleDataStream = (DataStream<Rule>) getSource(props, env, SOURCE_RULE_TAG);
        DataStream<Metric> metricDataStream = (DataStream<Metric>) getSource(props, env, SOURCE_METRIC_TAG);
        BroadcastStream<Rule> rulesStream = ruleDataStream.broadcast(ExecutionDescriptor.rulesDescriptor);

        // create execute
        DataStream<Result> resultDataStream =
                metricDataStream
                        .connect(rulesStream)
                        .process(new DynamicPartitionProcessFunction())
                        .name("Dynamic Partitioning Function")
                        .keyBy(1)
                        .connect(rulesStream)
                        .process(new DynamicApplyRuleProcessFunction())
                        .name("Dynamic Apply Rule Function");

        resultDataStream.print();

        env.execute("Dynamic Cep Engine");
    }
}
