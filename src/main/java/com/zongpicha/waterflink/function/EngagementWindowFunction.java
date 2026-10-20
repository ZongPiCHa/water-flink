package com.zongpicha.waterflink.function;

import java.util.Iterator;
import java.util.HashMap;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.zongpicha.waterflink.model.EngagementAccumulator;
import com.zongpicha.waterflink.model.EngagementStats;
import com.zongpicha.waterflink.model.StatsScope;
import com.zongpicha.waterflink.model.WindowType;

public final class EngagementWindowFunction
        extends ProcessWindowFunction<
                EngagementAccumulator,
                EngagementStats,
                String,
                TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final StatsScope scope;
    private final WindowType windowType;

    public EngagementWindowFunction(StatsScope scope, WindowType windowType) {
        this.scope = scope;
        this.windowType = windowType;
    }

    @Override
    public void process(
            String key,
            Context context,
            Iterable<EngagementAccumulator> values,
            Collector<EngagementStats> output) {
        Iterator<EngagementAccumulator> iterator = values.iterator();
        if (!iterator.hasNext()) {
            return;
        }

        EngagementAccumulator accumulator = iterator.next();
        String userId = scope == StatsScope.USER
                ? key
                : accumulator.getTargetUserId();
        String projectId = scope == StatsScope.PROJECT
                ? key
                : null;

        EngagementStats stats = new EngagementStats(
                scope,
                windowType,
                userId,
                projectId,
                context.window().getStart(),
                context.window().getEnd(),
                accumulator.getLikes(),
                accumulator.getComments(),
                accumulator.getShares(),
                accumulator.getFollows());
        if (accumulator.getExtraMetrics() != null) {
            stats.setExtraMetrics(new HashMap<>(accumulator.getExtraMetrics()));
        }
        output.collect(stats);
    }
}
