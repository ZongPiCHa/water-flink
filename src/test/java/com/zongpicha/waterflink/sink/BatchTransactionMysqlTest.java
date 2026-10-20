package com.zongpicha.waterflink.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class BatchTransactionMysqlTest {
    @Test
    void realMysqlCommitsWholeBatchAndRollsBackPartialFailure() throws Exception {
        Assumptions.assumeTrue("1".equals(System.getenv("VERIFY_MYSQL_SINK")));
        String table = System.getenv("MYSQL_SINK_TEST_TABLE");
        Assumptions.assumeTrue(table != null && table.matches("flink_sink_batch_verify_[0-9]+"));
        String url = System.getenv("MYSQL_JDBC_URL");
        String user = System.getenv("MYSQL_USER");
        String password = System.getenv("MYSQL_PASSWORD");
        var options = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url).withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(user).withPassword(password).build();
        String query = EngagementJdbcSinks.currentRuleQuery(EngagementJdbcSinks.PROJECT_UPSERT)
                .replace("project_engagement_stats", table);
        try (Connection observer = DriverManager.getConnection(url, user, password);
                var provider = new BatchTransactionConnectionProvider(options)) {
            long version;
            long viewWeight;
            try (var statement = observer.createStatement();
                    var result = statement.executeQuery(
                            "SELECT version,view_page_weight FROM heat_score_rule WHERE status=1")) {
                assertTrue(result.next());
                version = result.getLong(1);
                viewWeight = result.getLong(2);
            }
            Connection connection = provider.getOrEstablishConnection();
            assertFalse(connection.getAutoCommit());
            try (var statement = connection.prepareStatement(query)) {
                bind(statement, "batch-check", version - 1, 2);
                statement.addBatch();
                bind(statement, "batch-check", version - 1, 3);
                statement.addBatch();
                assertEquals(0, scalar(observer, "SELECT COUNT(*) FROM " + table));
                statement.executeBatch();
                assertEquals(3, scalar(observer,
                        "SELECT view_page_count FROM " + table + " WHERE project_id='batch-check'"));
                assertEquals(3 * viewWeight, scalar(observer,
                        "SELECT heat_score FROM " + table + " WHERE project_id='batch-check'"));
                assertEquals(version, scalar(observer,
                        "SELECT score_version FROM " + table + " WHERE project_id='batch-check'"));

                bind(statement, "batch-check", version, 999);
                statement.addBatch();
                bind(statement, null, version, 1); // A required primary-key column.
                statement.addBatch();
                assertThrows(SQLException.class, statement::executeBatch);
                assertEquals(3, scalar(observer,
                        "SELECT view_page_count FROM " + table + " WHERE project_id='batch-check'"));
                assertFalse(provider.isConnectionValid());
            }
            connection = provider.getOrEstablishConnection();
            assertFalse(connection.getAutoCommit(), "Reconnect must restore transaction mode");
            try (var statement = connection.prepareStatement(query)) {
                bind(statement, "batch-check", version, 4);
                statement.addBatch();
                statement.executeBatch();
                assertEquals(4, scalar(observer,
                        "SELECT view_page_count FROM " + table + " WHERE project_id='batch-check'"));
            }

            // Same SQL, table and number of rows; only the transaction boundary differs.
            try (var seed = connection.prepareStatement(query)) {
                for (int i = 0; i < 300; i++) {
                    bind(seed, "benchmark-" + i, version, 0);
                    seed.addBatch();
                }
                seed.executeBatch();
            }
            long automaticMs;
            try (var automatic = observer.prepareStatement(query)) {
                for (int i = 0; i < 300; i++) {
                    bind(automatic, "benchmark-" + i, version, 1);
                    automatic.addBatch();
                }
                long start = System.nanoTime();
                automatic.executeBatch();
                automaticMs = (System.nanoTime() - start) / 1_000_000;
            }
            long transactionMs;
            try (var transaction = connection.prepareStatement(query)) {
                for (int i = 0; i < 300; i++) {
                    bind(transaction, "benchmark-" + i, version, 2);
                    transaction.addBatch();
                }
                long start = System.nanoTime();
                transaction.executeBatch();
                transactionMs = (System.nanoTime() - start) / 1_000_000;
            }
            assertEquals(300, scalar(observer,
                    "SELECT COUNT(*) FROM " + table + " WHERE project_id LIKE 'benchmark-%' AND view_page_count=2"));
            System.out.printf("MYSQL_BATCH_BENCHMARK rows=300 autoCommitMs=%d batchTransactionMs=%d%n",
                    automaticMs, transactionMs);
        }
    }

    private static long scalar(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private static void bind(PreparedStatement statement, String id, long version, long views)
            throws SQLException {
        statement.setInt(1, 1);
        statement.setTimestamp(2, Timestamp.valueOf("2030-01-01 00:00:00"));
        statement.setTimestamp(3, Timestamp.valueOf("2030-01-01 01:00:00"));
        statement.setString(4, id);
        statement.setString(5, "transaction-verification");
        for (int i = 6; i <= 10; i++) statement.setLong(i, 0);
        statement.setLong(11, version);
        statement.setString(12, "transaction verification");
        statement.setString(13, "transaction verification");
        statement.setTimestamp(14, null);
        statement.setLong(15, views);
    }
}
