package com.zongpicha.waterflink.sink;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;

import com.zongpicha.waterflink.model.EngagementStats;

public final class EngagementJdbcSinks {

    private static final ZoneId WINDOW_ZONE = ZoneId.of("Asia/Shanghai");

    static final String USER_UPSERT = """
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
                score_version,
                user_nickname,
                view_page_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS incoming
            ON DUPLICATE KEY UPDATE
                window_end = incoming.window_end,
                user_nickname = COALESCE(
                    incoming.user_nickname,
                    user_engagement_stats.user_nickname
                ),
                like_count = incoming.like_count,
                comment_count = incoming.comment_count,
                share_count = incoming.share_count,
                follow_count = incoming.follow_count,
                view_page_count = incoming.view_page_count,
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

    static final String PROJECT_UPSERT = """
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
                score_version,
                project_name,
                author_nickname,
                project_published_at,
                view_page_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS incoming
            ON DUPLICATE KEY UPDATE
                window_end = incoming.window_end,
                user_id = incoming.user_id,
                project_name = COALESCE(
                    incoming.project_name,
                    project_engagement_stats.project_name
                ),
                author_nickname = COALESCE(
                    incoming.author_nickname,
                    project_engagement_stats.author_nickname
                ),
                project_published_at = COALESCE(
                    incoming.project_published_at,
                    project_engagement_stats.project_published_at
                ),
                like_count = incoming.like_count,
                comment_count = incoming.comment_count,
                share_count = incoming.share_count,
                follow_count = incoming.follow_count,
                view_page_count = incoming.view_page_count,
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
                .withQueryStatement(currentRuleQuery(USER_UPSERT), (statement, stats) -> {
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
                    statement.setString(11, stats.getUserNickname());
                    statement.setLong(12, stats.metricValue("view_page"));
                })
                .withExecutionOptions(executionOptions())
                .buildAtLeastOnce(new BatchTransactionConnectionProvider(
                        connectionOptions(jdbcUrl, username, password)));
    }

    public static JdbcSink<EngagementStats> projectSink(
            String jdbcUrl,
            String username,
            String password) {
        return JdbcSink.<EngagementStats>builder()
                .withQueryStatement(currentRuleQuery(PROJECT_UPSERT), (statement, stats) -> {
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
                    statement.setString(12, stats.getProjectName());
                    statement.setString(13, stats.getUserNickname());
                    statement.setTimestamp(
                            14,
                            stats.getProjectPublishedAt() == null
                                    ? null
                                    : Timestamp.valueOf(stats.getProjectPublishedAt()));
                    statement.setLong(15, stats.metricValue("view_page"));
                })
                .withExecutionOptions(executionOptions())
                .buildAtLeastOnce(new BatchTransactionConnectionProvider(
                        connectionOptions(jdbcUrl, username, password)));
    }

    // Preserve Flink's normal calculation. Only late snapshots computed under an
    // older rule are recalculated at the database boundary, including new rows.
    // Count columns are explicitly chosen in each INSERT/sink binding above.
    static String currentRuleQuery(String upsert) {
        int columnsEnd = upsert.indexOf(") VALUES");
        String[] columns = upsert.substring(upsert.indexOf('(') + 1, columnsEnd)
                .trim().split("\\s*,\\s*");
        String facts = Arrays.stream(columns)
                .map(column -> "? AS " + column).collect(Collectors.joining(", "));
        String counts = Arrays.stream(columns).filter(column -> column.endsWith("_count"))
                .map(column -> "'" + column.substring(0, column.length() - 6)
                        + "', facts." + column)
                .collect(Collectors.joining(", "));
        String recalculate = """
                (SELECT COALESCE(SUM(
                    COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(JSON_OBJECT(%s),
                        CONCAT('$.', metric))) AS SIGNED), 0)
                    * CAST(JSON_UNQUOTE(JSON_EXTRACT(rule.weights,
                        CONCAT('$.', metric))) AS SIGNED)), 0)
                 FROM JSON_TABLE(JSON_KEYS(rule.weights), '$[*]'
                     COLUMNS(metric VARCHAR(48) PATH '$')) AS rule_metrics)
                """.formatted(counts);
        String normalized = Arrays.stream(columns).map(column -> switch (column) {
            case "heat_score" -> "IF(COALESCE(rule.version, 0) > facts.score_version, "
                    + recalculate + ", facts.heat_score) AS heat_score";
            case "score_version" -> "GREATEST(COALESCE(rule.version, 0), facts.score_version)"
                    + " AS score_version";
            default -> "facts." + column;
        }).collect(Collectors.joining(", "));
        return upsert.substring(0, columnsEnd + 1)
                + " SELECT * FROM (SELECT " + normalized
                + " FROM (SELECT " + facts + ") AS facts"
                + " LEFT JOIN (SELECT version, weights FROM heat_score_rule WHERE status = 1)"
                + " AS rule ON TRUE) AS incoming "
                + upsert.substring(upsert.indexOf("ON DUPLICATE KEY UPDATE"));
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
