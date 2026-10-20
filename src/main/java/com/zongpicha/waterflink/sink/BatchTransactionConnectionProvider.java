package com.zongpicha.waterflink.sink;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;

/**
 * Commits each JDBC executeBatch before the stock Flink executor can clear its
 * record buffer. The stock writer still owns batching, retries and checkpoint
 * flushing, and retains its existing operator/state serializers.
 *
 * <p>Only for the at-least-once prepared-statement sink. This is not XA: a lost
 * commit response can replay the batch, so queries must remain replay-safe.
 */
final class BatchTransactionConnectionProvider implements JdbcConnectionProvider {
    private static final long serialVersionUID = 1L;

    private final JdbcConnectionProvider delegate;
    private transient Connection connection;

    BatchTransactionConnectionProvider(JdbcConnectionOptions options) {
        this(new SimpleJdbcConnectionProvider(options));
    }

    BatchTransactionConnectionProvider(JdbcConnectionProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return connection != null && delegate.isConnectionValid();
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (isConnectionValid()) {
            return connection;
        }
        closeConnection();
        Connection physical = delegate.getOrEstablishConnection();
        try {
            physical.setAutoCommit(false);
            connection = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, args) -> {
                        Object result = invoke(physical, method, args);
                        if (method.getName().equals("prepareStatement")) {
                            return transactionalStatement(physical, (PreparedStatement) result);
                        }
                        return result;
                    });
            return connection;
        } catch (SQLException error) {
            closeConnection();
            throw error;
        }
    }

    private PreparedStatement transactionalStatement(
            Connection physical, PreparedStatement statement) {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    if (!method.getName().equals("executeBatch")) {
                        return invoke(statement, method, args);
                    }
                    try {
                        int[] results = statement.executeBatch();
                        for (int result : results) {
                            if (result == Statement.EXECUTE_FAILED) {
                                throw new SQLException("JDBC batch contained a failed statement");
                            }
                        }
                        physical.commit();
                        return results;
                    } catch (SQLException error) {
                        try {
                            physical.rollback();
                        } catch (SQLException rollbackError) {
                            error.addSuppressed(rollbackError);
                        } finally {
                            // Discard uncertain transactions and driver batch buffers.
                            // Flink retains the original records and rebinds the full
                            // batch on a fresh connection before accepting more input.
                            closeConnection();
                        }
                        throw error;
                    }
                });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException error) {
            throw error.getCause();
        }
    }

    @Override
    public void closeConnection() {
        connection = null;
        delegate.closeConnection();
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }
}
