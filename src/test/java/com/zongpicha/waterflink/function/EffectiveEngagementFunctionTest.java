package com.zongpicha.waterflink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import com.zongpicha.waterflink.model.EngagementEvent;
import com.zongpicha.waterflink.model.MetricDelta;
import com.zongpicha.waterflink.model.EngagementAccumulator;

class EffectiveEngagementFunctionTest {

    @Test
    void countsViewsAsPvAndMergesGenericMetrics() throws Exception {
        List<MetricDelta> output = run(
                like(2_000L, "view_page"), like(1_000L, "view_page"),
                like(3_000L, "download"), like(4_000L, "like"));
        EngagementAccumulator acc = new EngagementAccumulator();
        output.forEach(acc::add);
        EngagementAccumulator merged = new EngagementAccumulator();
        merged.merge(acc);
        assertEquals(2L, merged.getExtraMetrics().get("view_page"));
        assertEquals(1L, merged.getExtraMetrics().get("download"));
        assertEquals(1L, merged.getLikes());
    }

    @Test
    void ignoresLikeThatArrivesAfterNewerUnlike() throws Exception {
        List<MetricDelta> output = run(
                like(2_000L, "unlike"),
                like(1_000L, "like"));

        assertEquals(0L, sumLikes(output));
        assertEquals(0, output.size());
    }

    @Test
    void appliesInOrderToggles() throws Exception {
        List<MetricDelta> output = run(
                like(1_000L, "like"),
                like(2_000L, "unlike"),
                like(3_000L, "like"));

        assertEquals(1L, sumLikes(output));
        assertEquals(3, output.size());
    }

    @Test
    void ignoresStaleUnlikeAfterNewerLike() throws Exception {
        List<MetricDelta> output = run(
                like(1_000L, "like"),
                like(3_000L, "like"),
                like(2_000L, "unlike"));

        assertEquals(1L, sumLikes(output));
    }

    @Test
    void sharesAreNeverDeduplicatedByTime() throws Exception {
        List<MetricDelta> output = run(
                new EngagementEvent(2_000L, "log-1", "share", "actor", "author", "project", null),
                new EngagementEvent(1_000L, "log-1", "share", "actor", "author", "project", null));

        assertEquals(2, output.size());
    }

    private static EngagementEvent like(long timestamp, String action) {
        return new EngagementEvent(
                timestamp, "log-" + timestamp, action, "actor", "author", "project", null);
    }

    private static long sumLikes(List<MetricDelta> output) {
        return output.stream().mapToLong(MetricDelta::getLikeDelta).sum();
    }

    private static List<MetricDelta> run(EngagementEvent... events) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, EngagementEvent, MetricDelta> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new EffectiveEngagementFunction()),
                        EngagementEvent::relationKey,
                        Types.STRING)) {
            harness.open();
            for (EngagementEvent event : events) {
                harness.processElement(event, event.getTimestamp());
            }
            return harness.extractOutputValues();
        }
    }
}
