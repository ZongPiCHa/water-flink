package com.zongpicha.waterflink.sink;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;

import com.zongpicha.waterflink.model.EngagementStats;

public final class EngagementJdbcSinks {

    private static final ZoneId WINDOW_ZONE = ZoneId.of("Asia/Shanghai");

    private static final String USER_UPSERT = """
            INSERT INTO user_engagement_stats (
                window_type,
                window_start,
                window_end,
                user_id,
                like_count,
                comment_count,
                share_count,
                follow_count,
                heat_score,
                score_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS incoming
            ON DUPLICATE KEY UPDATE
                window_end = incoming.window_end,
                like_count = incoming.like_count,
                comment_count = incoming.comment_count,
                share_count = incoming.share_count,
                follow_count = incoming.follow_count,
                heat_score = IF(
                    incoming.score_version >= user_engagement_stats.score_version,
                    incoming.heat_score,
                    user_engagement_stats.heat_score
                ),
                score_version = GREATEST(
                    user_engagement_stats.score_version,
                    incoming.score_version
                ),
                updated_at = CURRENT_TIMESTAMP(3)
            """;

    private static final String PROJECT_UPSERT = """
            INSERT INTO project_engagement_stats (
                window_type,
                window_start,
                window_end,
                project_id,
                user_id,
                like_count,
                comment_count,
                share_count,
                follow_count,
                heat_score,
                score_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS incoming
            ON DUPLICATE KEY UPDATE
                window_end = incoming.window_end,
                user_id = incoming.user_id,
                like_count = incoming.like_count,
                comment_count = incoming.comment_count,
                share_count = incoming.share_count,
                follow_count = incoming.follow_count,
                heat_score = IF(
                    incoming.score_version >= project_engagement_stats.score_version,
                    incoming.heat_score,
                    project_engagement_stats.heat_score
                ),
                score_version = GREATEST(
                    project_engagement_stats.score_version,
                    incoming.score_version
                ),
                updated_at = CURRENT_TIMESTAMP(3)
            """;

    private EngagementJdbcSinks() {
    }

    public static JdbcSink<EngagementStats> userSink(
            String jdbcUrl,
            String username,
            String password) {
        return JdbcSink.<EngagementStats>builder()
                .withQueryStatement(USER_UPSERT, (statement, stats) -> {
                    statement.setInt(1, stats.getWindowType().getCode());
                    statement.setTimestamp(2, timestamp(stats.getWindowStart()));
                    statement.setTimestamp(3, timestamp(stats.getWindowEnd()));
                    statement.setString(4, stats.getUserId());
                    statement.setLong(5, stats.getLikes());
                    statement.setLong(6, stats.getComments());
                    statement.setLong(7, stats.getShares());
                    statement.setLong(8, stats.getFollows());
                    statement.setLong(9, stats.getHeatScore());
                    statement.setLong(10, stats.getScoreVersion());
                })
                .withExecutionOptions(executionOptions())
                .buildAtLeastOnce(connectionOptions(jdbcUrl, username, password));
    }

    public static JdbcSink<EngagementStats> projectSink(
            String jdbcUrl,
            String username,
            String password) {
        return JdbcSink.<EngagementStats>builder()
                .withQueryStatement(PROJECT_UPSERT, (statement, stats) -> {
                    statement.setInt(1, stats.getWindowType().getCode());
                    statement.setTimestamp(2, timestamp(stats.getWindowStart()));
                    statement.setTimestamp(3, timestamp(stats.getWindowEnd()));
                    statement.setString(4, stats.getProjectId());
                    statement.setString(5, stats.getUserId());
                    statement.setLong(6, stats.getLikes());
                    statement.setLong(7, stats.getComments());
                    statement.setLong(8, stats.getShares());
                    statement.setLong(9, stats.getFollows());
                    statement.setLong(10, stats.getHeatScore());
                    statement.setLong(11, stats.getScoreVersion());
                })
                .withExecutionOptions(executionOptions())
                .buildAtLeastOnce(connectionOptions(jdbcUrl, username, password));
    }

    private static JdbcExecutionOptions executionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(1000L)
                .withMaxRetries(3)
                .build();
    }

    private static JdbcConnectionOptions connectionOptions(
            String jdbcUrl,
            String username,
            String password) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(username)
                .withPassword(password)
                .withConnectionCheckTimeoutSeconds(10)
                .build();
    }

    private static Timestamp timestamp(long epochMillis) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(epochMillis),
                WINDOW_ZONE);
        return Timestamp.valueOf(localDateTime);
    }
}
