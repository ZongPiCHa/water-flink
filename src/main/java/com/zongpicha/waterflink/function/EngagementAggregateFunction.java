package com.zongpicha.waterflink.function;

import org.apache.flink.api.common.functions.AggregateFunction;

import com.zongpicha.waterflink.model.EngagementAccumulator;
import com.zongpicha.waterflink.model.MetricDelta;

public final class EngagementAggregateFunction
        implements AggregateFunction<
                MetricDelta,
                EngagementAccumulator,
                EngagementAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public EngagementAccumulator createAccumulator() {
        return new EngagementAccumulator();
    }

    @Override
    public EngagementAccumulator add(
            MetricDelta delta,
            EngagementAccumulator accumulator) {
        accumulator.add(delta);
        return accumulator;
    }

    @Override
    public EngagementAccumulator getResult(EngagementAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public EngagementAccumulator merge(
            EngagementAccumulator left,
            EngagementAccumulator right) {
        left.merge(right);
        return left;
    }
}
