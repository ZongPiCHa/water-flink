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
        TimeWindow window = assignWindow(
                Duration.ofDays(1),
                Duration.ofHours(-8),
                "2026-10-14T07:30:00Z");

        assertEquals(
                Instant.parse("2026-10-13T16:00:00Z").toEpochMilli(),
                window.getStart());
        assertEquals(
                Instant.parse("2026-10-14T16:00:00Z").toEpochMilli(),
                window.getEnd());
    }

    @Test
    void alignsThreeDayWindowToFixedShanghaiDateBlocks() {
        TimeWindow window = assignWindow(
                Duration.ofDays(3),
                Duration.ofHours(-8),
                "2026-10-14T07:30:00Z");

        assertEquals(
                Instant.parse("2026-10-12T16:00:00Z").toEpochMilli(),
                window.getStart());
        assertEquals(
                Instant.parse("2026-10-15T16:00:00Z").toEpochMilli(),
                window.getEnd());
    }

    @Test
    void alignsWeekWindowToShanghaiMonday() {
        TimeWindow window = assignWindow(
                Duration.ofDays(7),
                Duration.ofHours(-80),
                "2026-10-14T07:30:00Z");

        assertEquals(
                Instant.parse("2026-10-11T16:00:00Z").toEpochMilli(),
                window.getStart());
        assertEquals(
                Instant.parse("2026-10-18T16:00:00Z").toEpochMilli(),
                window.getEnd());
    }

    private static TimeWindow assignWindow(
            Duration size,
            Duration offset,
            String eventInstant) {
        TumblingEventTimeWindows assigner =
                TumblingEventTimeWindows.of(size, offset);
        Collection<TimeWindow> windows = assigner.assignWindows(
                new Object(),
                Instant.parse(eventInstant).toEpochMilli(),
                new WindowAssigner.WindowAssignerContext() {
                    @Override
                    public long getCurrentProcessingTime() {
                        return 0L;
                    }
                });
        return windows.iterator().next();
    }
}
