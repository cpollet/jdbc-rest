package net.cpollet.jdbcrest;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

import com.google.common.truth.Truth;

/**
 * (c) Swissquote 30.11.17
 *
 * @author cpollet
 */
public class TestRestDriver {
	@Test
	public void acceptsURL_returnsTrue_whenValid() throws SQLException {
		RestDriver driver = new RestDriver();

		boolean acceptsUrl = driver.acceptsURL("jdbc:rest://localhost:8000/database");

		Truth.assertThat(acceptsUrl).isTrue();
	}

	@Test
	public void acceptsURL_returnsFalse_whenSchemaNotSupported() throws SQLException {
		RestDriver driver = new RestDriver();

		boolean acceptsUrl = driver.acceptsURL("jdbc:invalid://localhost:8000/database");

		Truth.assertThat(acceptsUrl).isFalse();
	}

	@Test
	public void acceptsURL_returnsFalse_whenDatabaseIsMissing() throws SQLException {
		RestDriver driver = new RestDriver();

		boolean acceptsUrl = driver.acceptsURL("jdbc:invalid://localhost:8000/");

		Truth.assertThat(acceptsUrl).isFalse();
	}

	@Test
	public void acceptsURL_returnsFalse_whenHostIsMissing() throws SQLException {
		RestDriver driver = new RestDriver();

		boolean acceptsUrl = driver.acceptsURL("jdbc:invalid:///database");

		Truth.assertThat(acceptsUrl).isFalse();
	}

	@Test
	public void connect_returnsNull_whenUrlIsInvalid() throws SQLException {
		RestDriver driver = new RestDriver();

		Connection connection = driver.connect("jdbc:invalid://localhost:8000/", null);

		Truth.assertThat(connection).isNull();
	}

	@Test
	public void connect_returnsNull_whenUrlIsValid() throws SQLException {
		RestDriver driver = new RestDriver();

		Connection connection = driver.connect("jdbc:rest://localhost:8000/database", null);

		Truth.assertThat(connection).isNotNull();
	}

	@Test
	public void connect_returnsAUniqueConnectionInstance_whencalledMultipleTimes() throws SQLException {
		RestDriver driver = new RestDriver();

		Connection connection1 = driver.connect("jdbc:rest://localhost:8000/database", null);
		Connection connection2 = driver.connect("jdbc:rest://localhost:8000/database", null);

		Truth.assertThat(connection1).isSameAs(connection2);
	}
}
