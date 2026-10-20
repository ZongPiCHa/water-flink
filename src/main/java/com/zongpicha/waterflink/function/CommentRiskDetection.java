package com.zongpicha.waterflink.function;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import com.zongpicha.waterflink.model.CommentRetraction;
import com.zongpicha.waterflink.model.CommentRiskAlert;
import com.zongpicha.waterflink.model.EngagementEvent;

/**
 * Comment spam / harassment detection: "post a comment, then delete it soon
 * after", repeated by the same user.
 *
 * <p>Stage 1 is an event-time interval join of the comment stream and the
 * delete_comment stream on comment_id. Only deletes by the comment's own
 * writer within {@code retractWithin} count as a retraction. Stage 2 is a CEP
 * pattern keyed by user: {@code riskCount} retractions within
 * {@code riskWindow}. Matches skip past the last event, so one burst of six
 * retractions yields two alerts rather than every overlapping combination.
 *
 * <p>State is bounded by both durations: the join keeps each side for at most
 * {@code retractWithin}, and partial CEP matches are pruned after
 * {@code riskWindow}.
 */
public final class CommentRiskDetection {

    private static final String RETRACT = "retract";

    private CommentRiskDetection() {
    }

    public static DataStream<CommentRiskAlert> detect(
            DataStream<EngagementEvent> events,
            Duration retractWithin,
            int riskCount,
            Duration riskWindow,
            int parallelism) {
        DataStream<EngagementEvent> comments = events
                .filter(event -> "comment".equals(event.getAction()))
                .name("Select comments")
                .uid("select-comments")
                .setParallelism(parallelism);
        DataStream<EngagementEvent> deletes = events
                .filter(event -> "delete_comment".equals(event.getAction()))
                .name("Select comment deletes")
                .uid("select-comment-deletes")
                .setParallelism(parallelism);

        DataStream<CommentRetraction> retractions = comments
                .keyBy(EngagementEvent::getCommentId)
                .intervalJoin(deletes.keyBy(EngagementEvent::getCommentId))
                .between(Duration.ZERO, retractWithin)
                .process(new RetractionJoinFunction())
                .name("Join comments with deletes")
                .uid("join-comment-retractions")
                .setParallelism(parallelism);

        return CEP.pattern(
                        retractions.keyBy(CommentRetraction::getUserId),
                        pattern(riskCount, riskWindow))
                .inEventTime()
                .process(new RiskAlertFunction())
                .name("Detect repeated comment retractions")
                .uid("detect-comment-risk")
                .setParallelism(parallelism);
    }

    static Pattern<CommentRetraction, CommentRetraction> pattern(
            int riskCount,
            Duration riskWindow) {
        return Pattern.<CommentRetraction>begin(
                        RETRACT,
                        AfterMatchSkipStrategy.skipPastLastEvent())
                .times(riskCount)
                .within(riskWindow);
    }

    static final class RetractionJoinFunction
            extends ProcessJoinFunction<EngagementEvent, EngagementEvent, CommentRetraction> {

        private static final long serialVersionUID = 1L;

        private transient Counter retractions;
        private transient Counter foreignDeletes;

        @Override
        public void open(OpenContext openContext) {
            retractions = getRuntimeContext().getMetricGroup().counter("comment_retractions");
            foreignDeletes = getRuntimeContext().getMetricGroup()
                    .counter("comment_foreign_deletes");
        }

        @Override
        public void processElement(
                EngagementEvent comment,
                EngagementEvent delete,
                Context context,
                Collector<CommentRetraction> output) {
            // A moderator or the project owner deleting someone else's comment is
            // not the writer retracting it.
            if (!comment.getActorUserId().equals(delete.getActorUserId())) {
                foreignDeletes.inc();
                return;
            }
            retractions.inc();
            output.collect(new CommentRetraction(comment, delete));
        }
    }

    static final class RiskAlertFunction
            extends PatternProcessFunction<CommentRetraction, CommentRiskAlert> {

        private static final long serialVersionUID = 1L;

        private transient Counter alerts;

        @Override
        public void open(OpenContext openContext) {
            alerts = getRuntimeContext().getMetricGroup().counter("comment_risk_alerts");
        }

        @Override
        public void processMatch(
                Map<String, List<CommentRetraction>> match,
                Context context,
                Collector<CommentRiskAlert> output) {
            List<CommentRetraction> retractions = match.get(RETRACT);
            CommentRetraction first = retractions.get(0);
            Set<String> commentIds = new LinkedHashSet<>();
            Set<String> projectIds = new LinkedHashSet<>();
            Set<String> authorIds = new LinkedHashSet<>();
            long totalLifetime = 0L;
            long maxLifetime = 0L;
            long lastDeletedAt = 0L;
            for (CommentRetraction retraction : retractions) {
                commentIds.add(retraction.getCommentId());
                projectIds.add(retraction.getProjectId());
                authorIds.add(retraction.getAuthorUserId());
                totalLifetime += retraction.lifetimeMillis();
                maxLifetime = Math.max(maxLifetime, retraction.lifetimeMillis());
                lastDeletedAt = Math.max(lastDeletedAt, retraction.getDeletedAt());
            }

            CommentRiskAlert alert = new CommentRiskAlert();
            alert.setUserId(first.getUserId());
            alert.setFirstCommentId(first.getCommentId());
            alert.setFirstCommentAt(first.getCommentedAt());
            alert.setLastDeletedAt(lastDeletedAt);
            alert.setRetractCount(retractions.size());
            alert.setProjectCount(projectIds.size());
            alert.setAuthorCount(authorIds.size());
            alert.setAvgLifetimeMillis(totalLifetime / retractions.size());
            alert.setMaxLifetimeMillis(maxLifetime);
            alert.setCommentIds(String.join(",", commentIds));
            alert.setProjectIds(String.join(",", projectIds));
            alerts.inc();
            output.collect(alert);
        }
    }
}
