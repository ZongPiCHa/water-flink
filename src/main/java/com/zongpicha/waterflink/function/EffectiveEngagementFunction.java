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

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("active-engagement", Boolean.class);
        descriptor.enableTimeToLive(StateTtlConfig
                .newBuilder(RELATION_STATE_TTL)
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .build());
        activeState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            EngagementEvent event,
            Context context,
            Collector<MetricDelta> output) throws Exception {
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
            case "share" -> output.collect(MetricDelta.share(event));
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
