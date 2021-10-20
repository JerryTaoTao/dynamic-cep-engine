package om.streamnative.cep.functions;

import com.alibaba.fastjson.JSONObject;
import com.streamnative.cep.common.DynamicFieldAccessor;
import com.streamnative.cep.common.entity.Metric;
import com.streamnative.cep.common.entity.Rule;
import com.streamnative.cep.common.field.accessor.ReflectFieldsAccessor;
import com.streamnative.cep.common.state.StateOperateHolder;
import om.streamnative.cep.ExecutionDescriptor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;


public class DynamicPartitionProcessFunction
        extends BroadcastProcessFunction<Metric, Rule, Tuple3<Metric, String, Integer>> implements StateOperateHolder {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionProcessFunction.class);
    //todo reflect can't get good process speed. will be replace by dynamic codegen later
    DynamicFieldAccessor<Metric, Map<String, String>> dynamicFieldAccessor = new ReflectFieldsAccessor();

    public DynamicPartitionProcessFunction() {

    }

    @Override
    public void processElement(Metric metric, ReadOnlyContext ctx, Collector<Tuple3<Metric, String, Integer>> out) throws Exception {

        ReadOnlyBroadcastState<Integer, Rule> readOnlyBroadcastState =
                ctx.getBroadcastState(ExecutionDescriptor.rulesDescriptor);
        StreamSupport
                .stream(readOnlyBroadcastState.immutableEntries().spliterator(), false)
                .forEach(integerRuleEntry -> {
                    Integer ruleId = integerRuleEntry.getKey();
                    Rule rule = integerRuleEntry.getValue();
                    if (isValidElement(metric, rule)) {
                        List<String> groupingKeyNames = rule.getGroupingKeyNames();
                        try {
                            List<Tuple2<String, String>> partitionKeyAndValueList = dynamicFieldAccessor
                                    .getPartitionFieldList(groupingKeyNames, metric);
                            String partitionInfo = getPartitionKeyAndValueJsonString(partitionKeyAndValueList);
                            Tuple3<Metric, String, Integer> record = Tuple3.of(metric, partitionInfo, ruleId);
                            out.collect(record);
                        } catch (Exception e) {
                            LOG.error("get partition key and value list with grouping list {} failed. ", groupingKeyNames, e);
                        }
                    }
                });

    }

    public boolean isValidElement(Metric metric, Rule rule) {
        if (metric == null) {
            return false;
        }
        List<String> groupingKeyNames = rule.getGroupingKeyNames();
        Set<String> allKeys = metric.getTags().keySet();
        return groupingKeyNames
                .stream()
                .allMatch(allKeys::contains);

    }

    public String getPartitionKeyAndValueJsonString(List<Tuple2<String, String>> partitionKeyAndValueList) {
        JSONObject result = new JSONObject();
        partitionKeyAndValueList
                .forEach(tuple2 -> {
                    result.put(tuple2.f0, tuple2.f1);
                });
        return result.toJSONString();
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Tuple3<Metric, String, Integer>> out) throws Exception {
        LOG.info("receive the rule {} .", rule);
        BroadcastState<Integer, Rule> ruleBroadCastState = ctx.getBroadcastState(ExecutionDescriptor.rulesDescriptor);
        processBroadCastRuleState(rule, ruleBroadCastState);
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
