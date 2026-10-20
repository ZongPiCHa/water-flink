package com.zongpicha.waterflink.function;

import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.zongpicha.waterflink.model.EngagementEvent;
import com.zongpicha.waterflink.model.MetricDelta;

public final class EffectiveEngagementFunction
        extends KeyedProcessFunction<String, EngagementEvent, MetricDelta> {

    private static final long serialVersionUID = 1L;
    private static final Duration RELATION_STATE_TTL = Duration.ofDays(30);

    private transient ValueState<Boolean> activeState;
    private transient ValueState<Long> lastEventTimestampState;

    @Override
    public void open(OpenContext openContext) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(RELATION_STATE_TTL)
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .build();

        ValueStateDescriptor<Boolean> activeDescriptor =
                new ValueStateDescriptor<>("active-engagement", Boolean.class);
        activeDescriptor.enableTimeToLive(ttlConfig);
        activeState = getRuntimeContext().getState(activeDescriptor);

        ValueStateDescriptor<Long> lastTimestampDescriptor =
                new ValueStateDescriptor<>(
                        "last-relation-event-timestamp",
                        Long.class);
        lastTimestampDescriptor.enableTimeToLive(ttlConfig);
        lastEventTimestampState =
                getRuntimeContext().getState(lastTimestampDescriptor);
    }

    @Override
    public void processElement(
            EngagementEvent event,
            Context context,
            Collector<MetricDelta> output) throws Exception {
        if ("share".equals(event.getAction())) {
            output.collect(MetricDelta.share(event));
            return;
        }
        switch (event.getAction()) {
            case "like", "unlike", "comment", "delete_comment", "follow" -> { }
            default -> {
                output.collect(MetricDelta.count(event));
                return;
            }
        }

        // Toggle actions are decided by the latest event in event time. An older
        // event arriving after a newer one (e.g. like after unlike) is stale and
        // must not flip the relation back.
        Long lastTimestamp = lastEventTimestampState.value();
        if (lastTimestamp != null && event.getTimestamp() < lastTimestamp) {
            return;
        }
        lastEventTimestampState.update(event.getTimestamp());

        boolean active = Boolean.TRUE.equals(activeState.value());

        switch (event.getAction()) {
            case "like" -> {
                if (!active) {
                    activeState.update(true);
                    output.collect(MetricDelta.like(event, 1L));
                }
            }
            case "unlike" -> {
                if (active) {
                    activeState.clear();
                    output.collect(MetricDelta.like(event, -1L));
                }
            }
            case "comment" -> {
                if (!active) {
                    activeState.update(true);
                    output.collect(MetricDelta.comment(event, 1L));
                }
            }
            case "delete_comment" -> {
                if (active) {
                    activeState.clear();
                    output.collect(MetricDelta.comment(event, -1L));
                }
            }
            case "follow" -> {
                if (!active) {
                    activeState.update(true);
                    output.collect(MetricDelta.follow(event));
                }
            }
            default -> throw new IllegalStateException(
                    "Unsupported action: " + event.getAction());
        }
    }
}
