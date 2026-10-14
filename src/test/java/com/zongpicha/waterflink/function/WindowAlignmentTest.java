package com.zongpicha.waterflink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.Test;

class WindowAlignmentTest {

    @Test
    void alignsDayWindowToShanghaiMidnight() {
        TumblingEventTimeWindows assigner = TumblingEventTimeWindows.of(
                Duration.ofDays(1),
                Duration.ofHours(-8));
        long eventTime = Instant.parse("2026-10-14T07:30:00Z").toEpochMilli();

        Collection<TimeWindow> windows = assigner.assignWindows(
                new Object(),
                eventTime,
                new WindowAssigner.WindowAssignerContext() {
                    @Override
                    public long getCurrentProcessingTime() {
                        return 0L;
                    }
                });
        TimeWindow window = windows.iterator().next();

        assertEquals(
                Instant.parse("2026-10-13T16:00:00Z").toEpochMilli(),
                window.getStart());
        assertEquals(
                Instant.parse("2026-10-14T16:00:00Z").toEpochMilli(),
                window.getEnd());
    }
}
