package com.zongpicha.waterflink.function;

import java.util.Iterator;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.zongpicha.waterflink.model.EngagementAccumulator;
import com.zongpicha.waterflink.model.UserEngagementStats;

public final class UserMinuteWindowFunction
        extends ProcessWindowFunction<
                EngagementAccumulator,
                UserEngagementStats,
                String,
                TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(
            String targetUserId,
            Context context,
            Iterable<EngagementAccumulator> values,
            Collector<UserEngagementStats> output) {
        Iterator<EngagementAccumulator> iterator = values.iterator();
        if (!iterator.hasNext()) {
            return;
        }

        EngagementAccumulator accumulator = iterator.next();
        output.collect(new UserEngagementStats(
                targetUserId,
                context.window().getStart(),
                context.window().getEnd(),
                accumulator.getLikes(),
                accumulator.getComments(),
                accumulator.getShares(),
                accumulator.getFollows()));
    }
}
