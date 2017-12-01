package net.cpollet.jdbcrest;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

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
}
