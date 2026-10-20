package com.zongpicha.waterflink.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriter;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.junit.jupiter.api.Test;

class BatchTransactionConnectionProviderTest {
    @Test
    void sizeAndCheckpointFlushCommitBeforeReturning() throws Exception {
        FakeDatabase db = new FakeDatabase();
        try (var writer = writer(db, 3, 0, 1)) {
            writer.write(1, null);
            writer.write(2, null);
            assertNull(db.value);
            writer.write(3, null);
            assertEquals(3, db.value);
            assertEquals(1, db.commits);
            writer.write(4, null);
            assertEquals(3, db.value);
            writer.flush(false); // The flush called at a checkpoint barrier.
            assertEquals(4, db.value);
            assertEquals(2, db.commits);
            writer.flush(false);
            assertEquals(2, db.commits, "An empty checkpoint must not commit again");
        }
        assertEquals(List.of(List.of(1, 2, 3), List.of(4)), db.batches);
    }

    @Test
    void timerCommitsBelowSizeThreshold() throws Exception {
        FakeDatabase db = new FakeDatabase();
        try (var writer = writer(db, 1000, 20, 1)) {
            writer.write(7, null);
            assertTrue(db.committed.await(5, TimeUnit.SECONDS));
            assertEquals(7, db.value);
            assertEquals(1, db.commits);
        }
    }

    @Test
    void partialBatchFailureRollsBackAndReplaysAllRecordsOnNewConnection() throws Exception {
        FakeDatabase db = new FakeDatabase();
        db.failBatch = true;
        try (var writer = writer(db, 3, 0, 1)) {
            writer.write(1, null);
            writer.write(2, null);
            writer.write(3, null);
            assertEquals(3, db.value);
            assertEquals(1, db.rollbacks);
            assertEquals(2, db.connections);
            assertEquals(List.of(false, false), db.autoCommitSettings);
            assertEquals(List.of(List.of(1, 2, 3), List.of(1, 2, 3)), db.batches);
        }
    }

    @Test
    void lostCommitResponseDoesNotDropTheBufferedBatch() throws Exception {
        FakeDatabase db = new FakeDatabase();
        db.loseCommitResponse = true;
        try (var writer = writer(db, 3, 0, 1)) {
            writer.write(1, null);
            writer.write(2, null);
            writer.write(3, null);
            assertEquals(3, db.value, "Replay uses full snapshot upserts");
            assertEquals(2, db.commits);
            assertEquals(1, db.rollbacks);
            assertEquals(2, db.connections);
            assertEquals(List.of(List.of(1, 2, 3), List.of(1, 2, 3)), db.batches);
        }
    }

    @Test
    void permanentFailureRejectsCheckpointAndDoesNotCommitPartialData() throws Exception {
        FakeDatabase db = new FakeDatabase();
        db.alwaysFail = true;
        var writer = writer(db, 1000, 0, 1);
        try {
            writer.write(1, null);
            writer.write(2, null);
            assertThrows(IOException.class, () -> writer.flush(false));
            assertNull(db.value);
            assertEquals(0, db.commits);
            assertEquals(2, db.rollbacks);
        } finally {
            // The stock writer also attempts its final flush; failure must surface.
            assertThrows(RuntimeException.class, writer::close);
        }
    }

    private JdbcWriter<Integer> writer(FakeDatabase db, int batchSize, long interval, int retries)
            throws IOException {
        InitContext context = (InitContext) Proxy.newProxyInstance(
                InitContext.class.getClassLoader(), new Class<?>[]{InitContext.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getRestoredCheckpointId")) {
                        return OptionalLong.empty();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        JdbcQueryStatement<Integer> query = new JdbcQueryStatement<>() {
            public String query() {
                return "INSERT INTO snapshots VALUES (1, ?) ON DUPLICATE KEY UPDATE value=VALUES(value)";
            }
            public void statement(PreparedStatement statement, Integer value) throws SQLException {
                statement.setInt(1, value);
            }
        };
        return new JdbcWriter<>(
                new BatchTransactionConnectionProvider(db),
                JdbcExecutionOptions.builder().withBatchSize(batchSize)
                        .withBatchIntervalMs(interval).withMaxRetries(retries).build(),
                null, query, JdbcOutputSerializer.of(IntSerializer.INSTANCE),
                DeliveryGuarantee.AT_LEAST_ONCE, Collections.emptyList(), context);
    }

    /** Minimal transactional database double; statements for one key carry full snapshots. */
    private static final class FakeDatabase implements JdbcConnectionProvider {
        volatile Integer value;
        int commits;
        int rollbacks;
        int connections;
        boolean failBatch;
        boolean alwaysFail;
        boolean loseCommitResponse;
        final CountDownLatch committed = new CountDownLatch(1);
        final List<List<Integer>> batches = new ArrayList<>();
        final List<Boolean> autoCommitSettings = new ArrayList<>();
        Connection connection;
        boolean closed = true;
        Integer staged;

        public Connection getConnection() { return connection; }
        public boolean isConnectionValid() { return !closed; }
        public void closeConnection() { closed = true; staged = null; }
        public Connection reestablishConnection() {
            closeConnection();
            return getOrEstablishConnection();
        }
        public Connection getOrEstablishConnection() {
            connections++;
            closed = false;
            connection = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "setAutoCommit":
                                autoCommitSettings.add((Boolean) args[0]);
                                return null;
                            case "prepareStatement":
                                return statement();
                            case "commit":
                                value = staged;
                                staged = null;
                                commits++;
                                if (loseCommitResponse) {
                                    loseCommitResponse = false;
                                    throw new SQLException("Commit succeeded but response was lost", "08006");
                                }
                                committed.countDown();
                                return null;
                            case "rollback":
                                rollbacks++;
                                staged = null;
                                return null;
                            default:
                                throw new UnsupportedOperationException(method.getName());
                        }
                    });
            return connection;
        }
        private PreparedStatement statement() {
            List<Integer> pending = new ArrayList<>();
            int[] parameter = {0};
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "setInt":
                                parameter[0] = (Integer) args[1];
                                return null;
                            case "addBatch":
                                pending.add(parameter[0]);
                                return null;
                            case "executeBatch":
                                if (closed) throw new SQLException("Closed connection", "08003");
                                batches.add(List.copyOf(pending));
                                staged = pending.get(0);
                                if (failBatch || alwaysFail) {
                                    failBatch = false;
                                    throw new SQLException("Failure after the first row", "40001");
                                }
                                staged = pending.get(pending.size() - 1);
                                int[] counts = new int[pending.size()];
                                java.util.Arrays.fill(counts, 1);
                                pending.clear();
                                return counts;
                            case "close":
                                pending.clear();
                                return null;
                            default:
                                throw new UnsupportedOperationException(method.getName());
                        }
                    });
        }
    }
}
