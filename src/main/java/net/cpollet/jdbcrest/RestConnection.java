package net.cpollet.jdbcrest;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * (c) Swissquote 30.11.17
 *
 * @author cpollet
 */
public class RestConnection implements Connection {
    private final String url;
    private final Properties info;
    private boolean autoCommit;
    private boolean isClosed;
    private boolean isReadonly;

    public RestConnection(String url, Properties info) {
        this.url = url;
        this.info = info;
        this.autoCommit = true;
        this.isClosed = false;
        this.isReadonly = false;
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        return new RestStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws
            SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        if (getAutoCommit()) {
            throw new SQLException("Cannot commit in auto commit mode");
        }
    }

    @Override
    public void rollback() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        if (getAutoCommit()) {
            throw new SQLException("Cannot rollback in auto commit mode");
        }
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null; // TODO
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        // FIXME goal: don't cache queries when in readonly as we don't need to replay them...
        this.isReadonly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        return isReadonly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // ignore
    }

    @Override
    public String getCatalog() throws SQLException {
        return null; // feature not implemented
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        final List<Integer> validTransactionLevels = Arrays.asList(TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED,
                TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE);

        if (!validTransactionLevels.contains(level)) {
            throw new SQLException(String.format("Unsupported level: %d", level));
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        return TRANSACTION_READ_UNCOMMITTED;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        return null; // feature not implemented
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        // feature not implemented
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !isClosed;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        //
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        //
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        // ignore
    }

    @Override
    public String getSchema() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed");
        }

        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isAssignableFrom(RestConnection.class)) {
            throw new SQLException(String.format("Cannot be unwrapped to %s", iface.getName()));
        }

        //noinspection unchecked
        return (T) this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
