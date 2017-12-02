package net.cpollet.jdbcrest;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.truth.Truth;

/**
 * (c) Swissquote 01.12.17
 *
 * @author cpollet
 */
public class TestRestConnection {
    @Test
    public void createStatement_returnsAStatementInstance() throws SQLException {
        Connection connection = new RestConnection(null, null);

        Statement statement = connection.createStatement();

        Truth.assertThat(statement).isNotNull();
    }

    @Test
    public void createStatement_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.createStatement();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void nativeSQL_returnsSameString() throws SQLException {
        Connection connection = new RestConnection(null, null);

        String nativeSQL = connection.nativeSQL("sql");

        Truth.assertThat(nativeSQL).isEqualTo("sql");
    }

    @Test
    public void nativeSQL_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.nativeSQL("sql");
            Assert.fail("Exception not thrown");
        } catch (Exception e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void getAutoCommit_returnsFalse_byDefault() throws SQLException {
        Connection connection = new RestConnection(null, null);

        boolean autoCommit = connection.getAutoCommit();

        Truth.assertThat(autoCommit).isTrue();
    }

    @Test
    public void setAutoCommit_configuresCommitMode() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.setAutoCommit(false);

        boolean autoCommit = connection.getAutoCommit();

        Truth.assertThat(autoCommit).isFalse();
    }

    @Ignore
    @Test
    public void setAutoCommit_commitsCurrentTransaction_whenAutoCommitIsEnabled() throws SQLException {
        //
    }

    @Test
    public void setAutoCommit_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.setAutoCommit(false);
            Assert.fail("Exception not thrown");
        } catch (Exception e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void commit_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.commit();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void commit_throwsException_whenInAutoCommitMode() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.setAutoCommit(true);

        try {
            connection.commit();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Cannot commit in auto commit mode");
        }
    }

    @Ignore
    @Test
    public void commit_commitsTheTransaction() throws SQLException {
        //
    }

    @Test
    public void rollback_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.rollback();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void rollback_throwsException_whenInAutoCommitMode() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.setAutoCommit(true);

        try {
            connection.rollback();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Cannot rollback in auto commit mode");
        }
    }

    @Ignore
    @Test
    public void rollback_rollsTheTransactionBack() throws SQLException {
        //
    }

    @Test
    public void isClosed_returnsFalse_byDefault() throws SQLException {
        Connection connection = new RestConnection(null, null);

        boolean isClosed = connection.isClosed();

        Truth.assertThat(isClosed).isFalse();
    }

    @Test
    public void isClosed_returnsTrue_afterCloseIsCalled() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        boolean isClosed = connection.isClosed();

        Truth.assertThat(isClosed).isTrue();
    }

    @Ignore
    @Test
    public void close_rollsTheTransactionBack() {
        //
    }

    @Test
    public void isValid_returnsTrue_whenConnectionIsOpen() throws SQLException {
        Connection connection = new RestConnection(null, null);

        boolean isValid = connection.isValid(0);

        Truth.assertThat(isValid).isTrue();
    }

    @Test
    public void isValid_returnsFalse_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        boolean isValid = connection.isValid(0);

        Truth.assertThat(isValid).isFalse();
    }

    @Test
    public void abort_closesConnection() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.abort(null);

        boolean isClosed = connection.isClosed();

        Truth.assertThat(isClosed).isTrue();
    }

    @Test
    public void setScheme_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.setSchema("");
            Assert.fail("Exception not thrown");
        } catch (Exception e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void getScheme_returnsNull() throws SQLException {
        Connection connection = new RestConnection(null, null);

        String scheme = connection.getSchema();
        Truth.assertThat(scheme).isNull();
    }

    @Test
    public void getScheme_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.getSchema();
            Assert.fail("Exception not thrown");
        } catch (Exception e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void getReadonly_returnsFalse_byDefault() throws SQLException {
        Connection connection = new RestConnection(null, null);

        boolean isReadonly = connection.isReadOnly();

        Truth.assertThat(isReadonly).isFalse();
    }

    @Test
    public void getReadonly_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.isReadOnly();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void setReadonly_configuresReadonlyMode() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.setReadOnly(true);

        boolean isReadonly = connection.isReadOnly();

        Truth.assertThat(isReadonly).isTrue();
    }

    @Test
    public void setReadonly_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.setReadOnly(false);
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Ignore
    @Test
    public void setReadonly_throwsException_whenInATransaction() {
        //
    }

    @Test
    public void setTransactionIsolation_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void setTransactionIsolation_throwsException_whenLevelIsInvalid() {
        Connection connection = new RestConnection(null, null);

        try {
            final int invalidLevel = 42;
            connection.setTransactionIsolation(invalidLevel);
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Unsupported level: 42");
        }
    }

    @Test
    public void getTransactionIsolation_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.getTransactionIsolation();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void getTransactionIsolation_returnsReadUncommitted_byDefault() throws SQLException {
        Connection connection = new RestConnection(null, null);

        int transactionIsolation = connection.getTransactionIsolation();

        Truth.assertThat(transactionIsolation).isEqualTo(Connection.TRANSACTION_READ_UNCOMMITTED);
    }

    @Test
    public void setTransactionIsolation_hasNoEffects() throws SQLException {
        Connection connection = new RestConnection(null, null);

        for (Integer isolationLevel : Arrays.asList(Connection.TRANSACTION_READ_UNCOMMITTED,
                Connection.TRANSACTION_READ_COMMITTED,
                Connection.TRANSACTION_REPEATABLE_READ,
                Connection.TRANSACTION_SERIALIZABLE)) {
            connection.setTransactionIsolation(isolationLevel);

            int actualTransactionLevel = connection.getTransactionIsolation();

            Truth.assertThat(actualTransactionLevel).isEqualTo(Connection.TRANSACTION_READ_UNCOMMITTED);
        }
    }

    @Test
    public void getWarnings_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            //noinspection ThrowableNotThrown
            connection.getWarnings();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void getWarnings_returnsNull() throws SQLException {
        Connection connection = new RestConnection(null, null);

        SQLWarning warnings = connection.getWarnings();

        Truth.assertThat((Object) warnings).isNull();
    }

    @Test
    public void clearWarnings_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.clearWarnings();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void setHoldability_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.setHoldability(0);
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Test
    public void getHoldability_throwsException_whenConnectionIsClosed() throws SQLException {
        Connection connection = new RestConnection(null, null);
        connection.close();

        try {
            connection.getHoldability();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Connection closed");
        }
    }

    @Ignore
    @Test
    public void getHoldability_returnsValue() {
        //
    }

    @Ignore
    @Test
    public void setClientInfo() {
        //
    }

    @Ignore
    @Test
    public void getClientInfo() {
        //
    }

    @Test
    public void unwrap_throwsException_whenInterfaceIsNotCompatible() {
        Connection connection = new RestConnection(null, null);

        try {
            connection.unwrap(String.class);
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Cannot be unwrapped to java.lang.String");
        }
    }

    @Test
    public void unwrap_returnsConnection_whenInferfaceCompatible() throws SQLException {
        Connection connection = new RestConnection(null, null);

        Connection unwrappedConnection = connection.unwrap(Connection.class);

        Truth.assertThat(unwrappedConnection).isSameAs(connection);
    }

    @Test
    public void isWrapperFor_returnsFalse() throws SQLException {
        Connection connection = new RestConnection(null, null);

        boolean isWrapperFor = connection.isWrapperFor(String.class);

        Truth.assertThat(isWrapperFor).isFalse();
    }
}
