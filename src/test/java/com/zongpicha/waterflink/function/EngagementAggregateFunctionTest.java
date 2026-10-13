package com.zongpicha.waterflink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.zongpicha.waterflink.model.EngagementAccumulator;
import com.zongpicha.waterflink.model.MetricDelta;

class EngagementAggregateFunctionTest {

    @Test
    void aggregatesNetChangesForOneWindow() {
        EngagementAggregateFunction function = new EngagementAggregateFunction();
        EngagementAccumulator accumulator = function.createAccumulator();

        function.add(new MetricDelta("user-1", "project-1", 1L, 0L, 0L, 0L), accumulator);
        function.add(new MetricDelta("user-1", "project-1", -1L, 0L, 0L, 0L), accumulator);
        function.add(new MetricDelta("user-1", "project-1", 0L, 1L, 1L, 0L), accumulator);
        function.add(new MetricDelta("user-1", null, 0L, 0L, 0L, 1L), accumulator);

        EngagementAccumulator result = function.getResult(accumulator);
        assertEquals(0L, result.getLikes());
        assertEquals(1L, result.getComments());
        assertEquals(1L, result.getShares());
        assertEquals(1L, result.getFollows());
    }
}
