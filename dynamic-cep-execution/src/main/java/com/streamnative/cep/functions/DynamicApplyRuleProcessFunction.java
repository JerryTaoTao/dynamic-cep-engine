package com.streamnative.cep.functions;

import com.streamnative.cep.ExecutionDescriptor;
import com.streamnative.cep.common.acc.AccumulatorHelper;
import com.streamnative.cep.common.entity.Metric;
import com.streamnative.cep.common.entity.Result;
import com.streamnative.cep.common.entity.Rule;
import com.streamnative.cep.common.state.StateOperateHolder;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * implement the rule apply logic.
 */
public class DynamicApplyRuleProcessFunction
        extends KeyedBroadcastProcessFunction<
        String, Tuple3<Metric, String, Integer>, Rule, Result> implements StateOperateHolder, AccumulatorHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicApplyRuleProcessFunction.class);
    private transient MapState<Long, List<Metric>> windowState;

    @Override
    public void open(Configuration parameters) {

        windowState = getRuntimeContext()
                .getMapState(ExecutionDescriptor.windowStateDescriptor);

    }

    @Override
    public void processElement(
            Tuple3<Metric, String, Integer> value, ReadOnlyContext ctx, Collector<Result> out)
            throws Exception {
        Metric metric = value.f0;
        long logtime = metric.getEventTime();
        Integer ruleId = value.f2;

        persistRecordToState(logtime, metric, windowState);

        ReadOnlyBroadcastState<Integer, Rule> readOnlyBroadcastState = ctx.getBroadcastState(ExecutionDescriptor.rulesDescriptor);

        Rule rule = readOnlyBroadcastState.get(ruleId);
        if (rule != null) {
            Rule.RuleState ruleState = rule.getRuleState();
            if (Rule.RuleState.ACTIVE.equals(ruleState)) {
                long windowStart = rule.getWindowStartFor(logtime);
                long cleanTime = logtime + 5 * 60 * 1000;
                //clean state every 5 minutes later
                ctx.timerService().registerEventTimeTimer(cleanTime);
                SimpleAccumulator<BigDecimal> aggregator = getAccumulator(rule.getAggregatorFunctionType());
                accWithState(windowState, windowStart, logtime, aggregator, rule);
                BigDecimal aggregateResult = aggregator.getLocalValue();
                boolean isTrigger = rule.apply(aggregateResult);
                if (isTrigger) {
                    Result result = new Result(ruleId, aggregateResult, rule);
                    out.collect(result);
                }
            }
        }

    }

    @Override
    public void processBroadcastElement(Rule value, Context ctx, Collector<Result> out) throws Exception {
        LOG.info("receive the rule in {} ", value, this.getClass().getCanonicalName());
        BroadcastState<Integer, Rule> broadcastState =
                ctx.getBroadcastState(ExecutionDescriptor.rulesDescriptor);
        processBroadCastRuleState(value, broadcastState);
    }

    @Override

    public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Result> out)
            throws Exception {

        ReadOnlyBroadcastState<Integer, Rule> ruleReadOnlyBroadcastState = ctx
                .getBroadcastState(ExecutionDescriptor.rulesDescriptor);

        //find longest window from ruleReadOnlyBroadcastState
        long maxWindow = 0L;
        Iterator<Map.Entry<Integer, Rule>> iterator = ruleReadOnlyBroadcastState
                .immutableEntries()
                .iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, Rule> cur = iterator.next();
            Rule rule = cur.getValue();
            long window = rule.getWindowMinutes();
            if (maxWindow < window) {
                maxWindow = window;
            }
        }

        //cleaning window
        cleanupWindowState(windowState, maxWindow, timestamp);

    }

}
