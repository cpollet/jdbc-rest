package net.cpollet.jdbcrest;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * (c) Swissquote 30.11.17
 *
 * @author cpollet
 */
public class RestDriver implements Driver {
	private final static Pattern urlFormat = Pattern.compile("^jdbc:rest://[.-_:/a-zA_Z0-9]+/[-_a-zA-Z0-9]+$");

	static {
		initialize();
	}

	private RestConnection connection;

	private static void initialize() {
		try {
			RestDriver driver = new RestDriver();
			DriverManager.registerDriver(driver);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}

		if (connection == null) {
			connection = new RestConnection(url, info);
		}

		return connection;
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return urlFormat.matcher(url).matches();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		return 1;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}
}
