package edu.gatech.chai.fhironfhirbase.database;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

@Component
public class DatabaseConfigurationImpl implements DatabaseConfiguration {

	private String targetDialect;
	private DataSource dataSource;
	private Connection connection;

	@Override
	public String getSqlRenderTargetDialect() {
		return this.targetDialect;
	}

	@Override
	public void setSqlRenderTargetDialect(String targetDialect) {
		this.targetDialect = targetDialect;
	}

	@Override
	public DataSource getDataSource() {
		return this.dataSource;
	}

	@Override
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public Connection getConnection() {
		setConnection(this.connection);

		return this.connection;
	}

	@Override
	public void setConnection(Connection connection) {
		try {
			if (connection == null || connection.isClosed()) {
				connection = dataSource.getConnection();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		this.connection = connection;
	}

}
