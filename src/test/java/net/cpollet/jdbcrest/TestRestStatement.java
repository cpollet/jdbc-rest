package net.cpollet.jdbcrest;

import com.google.common.truth.Truth;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestRestStatement {
    @Test
    public void isClosed_returnsFalse_byDefault() throws SQLException {
        Statement statement = new RestStatement();

        boolean isClosed = statement.isClosed();

        Truth.assertThat(isClosed).isFalse();
    }

    @Test
    public void close_closesTheStatement() throws SQLException {
        Statement statement = new RestStatement();
        statement.close();

        boolean isClosed = statement.isClosed();

        Truth.assertThat(isClosed).isTrue();
    }

    @Test
    public void executeQuery_throwsException_whenStatementIsClosed() throws SQLException {
        Statement statement = new RestStatement();
        statement.close();

        try {
            statement.executeQuery("sql");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Truth.assertThat(e.getMessage()).isEqualTo("Statement closed");
        }
    }

    @Test
    public void executeQuery_returnsResultSet() throws SQLException {
        Statement statement = new RestStatement();

        ResultSet resultSet = statement.executeQuery("sql");

        Truth.assertThat(resultSet).isNotNull();
    }

    @Ignore
    @Test
    public void executeUpdate() {
        //
    }
}
