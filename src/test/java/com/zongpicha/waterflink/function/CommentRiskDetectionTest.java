package com.zongpicha.waterflink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import com.zongpicha.waterflink.model.CommentRiskAlert;
import com.zongpicha.waterflink.model.EngagementEvent;

class CommentRiskDetectionTest {

    private static final long MINUTE = 60_000L;

    @Test
    void alertsOnRepeatedQuickRetractionsBySameUser() throws Exception {
        List<EngagementEvent> events = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            retract(events, "spammer", "c" + index, "p" + index, index * MINUTE, 30_000L);
        }

        List<CommentRiskAlert> alerts = run(events);

        assertEquals(1, alerts.size());
        CommentRiskAlert alert = alerts.get(0);
        assertEquals("spammer", alert.getUserId());
        assertEquals("c0", alert.getFirstCommentId());
        assertEquals(3, alert.getRetractCount());
        assertEquals(3, alert.getProjectCount());
        assertEquals(30_000L, alert.getAvgLifetimeMillis());
        assertEquals("c0,c1,c2", alert.getCommentIds());
    }

    @Test
    void ignoresSlowDeletesForeignDeletesAndSparseRetractions() throws Exception {
        List<EngagementEvent> events = new ArrayList<>();
        // Deleted after the 5-minute join bound: not a retraction.
        retract(events, "slow", "s0", "p", 0L, 6 * MINUTE);
        retract(events, "slow", "s1", "p", MINUTE, 30_000L);
        retract(events, "slow", "s2", "p", 2 * MINUTE, 30_000L);
        // Deleted by someone else (author / moderator).
        events.add(event(0L, "comment", "victim", "v0"));
        events.add(event(10_000L, "delete_comment", "owner", "v0"));
        retract(events, "victim", "v1", "p", MINUTE, 10_000L);
        retract(events, "victim", "v2", "p", 2 * MINUTE, 10_000L);
        // Three retractions, but spread beyond the 10-minute CEP window.
        retract(events, "sparse", "x0", "p", 0L, 10_000L);
        retract(events, "sparse", "x1", "p", 6 * MINUTE, 10_000L);
        retract(events, "sparse", "x2", "p", 12 * MINUTE, 10_000L);

        assertEquals(0, run(events).size());
    }

    @Test
    void doesNotEmitOverlappingAlertsForOneBurst() throws Exception {
        List<EngagementEvent> events = new ArrayList<>();
        for (int index = 0; index < 7; index++) {
            retract(events, "burst", "b" + index, "p", index * 10_000L, 1_000L);
        }

        List<CommentRiskAlert> alerts = run(events);

        assertEquals(2, alerts.size());
        assertEquals("b0", alerts.get(0).getFirstCommentId());
        assertEquals("b3", alerts.get(1).getFirstCommentId());
    }

    private static List<CommentRiskAlert> run(List<EngagementEvent> events) throws Exception {
        events.sort(Comparator.comparingLong(EngagementEvent::getTimestamp));
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStream<EngagementEvent> source = environment
                .fromData(events)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<EngagementEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, previous) -> event.getTimestamp()));

        List<CommentRiskAlert> alerts = new ArrayList<>();
        CommentRiskDetection.detect(
                        source, Duration.ofMinutes(5), 3, Duration.ofMinutes(10), 2)
                .executeAndCollect()
                .forEachRemaining(alerts::add);
        alerts.sort(Comparator.comparingLong(CommentRiskAlert::getFirstCommentAt));
        return alerts;
    }

    private static void retract(
            List<EngagementEvent> events,
            String user,
            String commentId,
            String projectId,
            long commentedAt,
            long lifetime) {
        events.add(new EngagementEvent(
                1_000L + commentedAt, "log-" + commentId, "comment",
                user, "author-" + projectId, projectId, commentId));
        events.add(new EngagementEvent(
                1_000L + commentedAt + lifetime, "log-del-" + commentId, "delete_comment",
                user, "author-" + projectId, projectId, commentId));
    }

    private static EngagementEvent event(
            long timestamp, String action, String user, String commentId) {
        return new EngagementEvent(
                1_000L + timestamp, "log-" + action + commentId, action,
                user, "author", "p", commentId);
    }
}
