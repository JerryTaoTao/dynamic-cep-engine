package com.streamnative.cep.common.state;

import com.streamnative.cep.common.entity.Metric;
import com.streamnative.cep.common.entity.Rule;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;

import java.math.BigDecimal;
import java.util.*;

/**
 * state operate handler for control the state
 */
public interface StateOperateHolder {

    /**
     * manage rule for broadcastState。
     *
     * @param rule
     * @param broadcastState
     * @throws Exception
     */
    default void processBroadCastRuleState(Rule rule, BroadcastState<Integer, Rule> broadcastState)
            throws Exception {
        Rule.RuleState state = rule.getRuleState();
        switch (state) {
            case DELETE:
                broadcastState.remove(rule.getRuleId());
                break;
            case ACTIVE:
            case PAUSE:
                broadcastState.put(rule.getRuleId(), rule);
                break;
            default:
                throw new RuntimeException("unsupport rule state " + state);
        }
    }

    /**
     * buffer record to state
     *
     * @param logtime
     * @param metric
     * @param mapState
     * @throws Exception
     */
    default void persistRecordToState(Long logtime, Metric metric, MapState<Long, List<Metric>> mapState)
            throws Exception {

        List<Metric> acc = mapState.contains(logtime)
                ? mapState.get(logtime) :
                new ArrayList<>();
        acc.add(metric);
        mapState.put(logtime, acc);
    }


    /**
     * judge logtime is contains in this window
     *
     * @param logtime
     * @param windowStart
     * @param windowEnd
     * @return
     */
    default boolean windowContains(Long logtime, Long windowStart, long windowEnd) {
        return logtime >= windowStart && logtime <= windowEnd;
    }

    /**
     * accumulator window data with specified aggregator
     *
     * @param windowState
     * @param windStart
     * @param winEnd
     * @param aggregator
     * @param rule
     * @throws Exception
     */
    default void accWithState(MapState<Long, List<Metric>> windowState, Long windStart, Long winEnd,
                              SimpleAccumulator<BigDecimal> aggregator, Rule rule) throws Exception {
        String aggFiledName = rule.getAggregateFieldName();
        for (Long logtime : windowState.keys()) {
            if (windowContains(logtime, windStart, winEnd)) {
                List<Metric> metricsInWindowState = windowState.get(logtime);
                for (Metric metric : metricsInWindowState) {
                    BigDecimal aggregatedValue = metric.getMetric(aggFiledName);
                    aggregator.add(aggregatedValue);
                }
            }
        }
    }

    /**
     * cleanup the record whose time is lower than curTime minus longest window
     *
     * @param windowState
     * @param logestWindow
     * @param curTimeStamp
     */
    default void cleanupWindowState(MapState<Long, List<Metric>> windowState, Long logestWindow, Long curTimeStamp) {
        try {
            Iterator<Long> times = windowState.keys().iterator();
            while (times.hasNext()) {
                Long timeInWindow = times.next();
                if (timeInWindow < curTimeStamp - logestWindow) {
                    times.remove();
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
