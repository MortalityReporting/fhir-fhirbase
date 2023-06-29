package edu.gatech.chai.fhironfhirbase.database;

import java.sql.Connection;

import javax.sql.DataSource;

public interface DatabaseConfiguration {
	public void setSqlRenderTargetDialect(String targetDialect);
	public String getSqlRenderTargetDialect();
	public DataSource getDataSource();
	public void setDataSource(DataSource dataSource);
	public Connection getConnection();
	public void setConnection(Connection connection);
}
