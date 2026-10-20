package com.zongpicha.waterflink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.DriverManager;
import java.sql.Timestamp;
import java.util.UUID;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class HeatRuleSinkTest {
    @Test
    void staleRuleSnapshotsUseCurrentWeightsForInsertAndUpdate() throws Exception {
        Assumptions.assumeTrue("1".equals(System.getenv("VERIFY_MYSQL_SINK")));
        try (var connection = DriverManager.getConnection(System.getenv("MYSQL_JDBC_URL"),
                System.getenv("MYSQL_USER"), System.getenv("MYSQL_PASSWORD"))) {
            connection.setAutoCommit(false);
            try {
                long version;
                long viewWeight;
                try (var statement = connection.createStatement();
                        var result = statement.executeQuery(
                                "SELECT version,view_page_weight FROM heat_score_rule WHERE status=1")) {
                    result.next();
                    version = result.getLong(1);
                    viewWeight = result.getLong(2);
                }
                String id = "verify-" + UUID.randomUUID();
                for (boolean project : new boolean[]{false, true}) {
                    String query = EngagementJdbcSinks.currentRuleQuery(project
                            ? EngagementJdbcSinks.PROJECT_UPSERT : EngagementJdbcSinks.USER_UPSERT);
                    for (long views : new long[]{2, 3}) {
                        try (var insert = connection.prepareStatement(query)) {
                            insert.setInt(1, 1);
                            insert.setTimestamp(2, Timestamp.valueOf("2030-01-01 00:00:00"));
                            insert.setTimestamp(3, Timestamp.valueOf("2030-01-01 01:00:00"));
                            insert.setString(4, id);
                            int offset = project ? 1 : 0;
                            if (project) insert.setString(5, id);
                            for (int index = 5 + offset; index <= 9 + offset; index++)
                                insert.setLong(index, 0);
                            insert.setLong(10 + offset, version - 1);
                            insert.setString(11 + offset, "verification");
                            if (project) {
                                insert.setString(13, "verification");
                                insert.setTimestamp(14, null);
                            }
                            insert.setLong(project ? 15 : 12, views);
                            insert.executeUpdate();
                        }
                        String table = project ? "project_engagement_stats" : "user_engagement_stats";
                        String key = project ? "project_id" : "user_id";
                        try (var select = connection.prepareStatement(
                                "SELECT view_page_count,heat_score,score_version FROM "
                                        + table + " WHERE " + key + "=?")) {
                            select.setString(1, id);
                            try (var result = select.executeQuery()) {
                                result.next();
                                assertEquals(views, result.getLong(1));
                                assertEquals(views * viewWeight, result.getLong(2));
                                assertEquals(version, result.getLong(3));
                            }
                        }
                    }
                }
            } finally {
                connection.rollback();
            }
        }
    }
}
