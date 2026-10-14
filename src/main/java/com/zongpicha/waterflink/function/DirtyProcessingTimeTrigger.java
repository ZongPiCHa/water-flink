package com.zongpicha.waterflink.function;

import java.time.Duration;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Emits a window snapshot only when new records arrived since its previous emission.
 */
public final class DirtyProcessingTimeTrigger<W extends Window> extends Trigger<Object, W> {

    private static final long serialVersionUID = 1L;

    private static final ValueStateDescriptor<Boolean> DIRTY_STATE =
            new ValueStateDescriptor<>("dirty-processing-time-trigger-dirty", Boolean.class);
    // Preserve the original trigger's state type and name for savepoint compatibility.
    private static final ReducingStateDescriptor<Long> FIRE_TIME_STATE =
            new ReducingStateDescriptor<>("fire-time", new Min(), Long.class);

    private final long intervalMillis;

    private DirtyProcessingTimeTrigger(Duration interval) {
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        this.intervalMillis = interval.toMillis();
    }

    public static <W extends Window> DirtyProcessingTimeTrigger<W> of(Duration interval) {
        return new DirtyProcessingTimeTrigger<>(interval);
    }

    @Override
    public TriggerResult onElement(
            Object element,
            long timestamp,
            W window,
            TriggerContext context) throws Exception {
        ValueState<Boolean> dirtyState = context.getPartitionedState(DIRTY_STATE);
        ReducingState<Long> fireTimeState = context.getPartitionedState(FIRE_TIME_STATE);
        dirtyState.update(true);

        if (fireTimeState.get() == null) {
            long nextFireTime = Math.min(
                    context.getCurrentProcessingTime() + intervalMillis,
                    window.maxTimestamp());
            fireTimeState.add(nextFireTime);
            context.registerProcessingTimeTimer(nextFireTime);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(
            long time,
            W window,
            TriggerContext context) throws Exception {
        ReducingState<Long> fireTimeState = context.getPartitionedState(FIRE_TIME_STATE);
        Long expectedFireTime = fireTimeState.get();
        if (expectedFireTime == null || expectedFireTime != time) {
            return TriggerResult.CONTINUE;
        }

        fireTimeState.clear();
        ValueState<Boolean> dirtyState = context.getPartitionedState(DIRTY_STATE);
        if (Boolean.TRUE.equals(dirtyState.value())) {
            dirtyState.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext context) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext context) throws Exception {
        ReducingState<Long> fireTimeState = context.getPartitionedState(FIRE_TIME_STATE);
        Long fireTime = fireTimeState.get();
        if (fireTime != null) {
            context.deleteProcessingTimeTimer(fireTime);
        }
        fireTimeState.clear();
        context.getPartitionedState(DIRTY_STATE).clear();
    }

    @Override
    public String toString() {
        return "DirtyProcessingTimeTrigger(" + intervalMillis + " ms)";
    }

    private static final class Min implements ReduceFunction<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long left, Long right) {
            return Math.min(left, right);
        }
    }
}
