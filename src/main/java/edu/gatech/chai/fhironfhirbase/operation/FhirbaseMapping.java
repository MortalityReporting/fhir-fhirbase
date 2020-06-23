package edu.gatech.chai.fhironfhirbase.operation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import edu.gatech.chai.fhironfhirbase.database.DatabaseConfiguration;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;


@Component
public class FhirbaseMapping implements IResourceMapping {
	private static final Logger logger = LoggerFactory.getLogger(FhirbaseMapping.class);

	@Autowired
	DatabaseConfiguration databaseConfiguration;

	protected FhirContext ctx;

	public FhirContext getCtx() {
		return this.ctx;
	}
	
	public void setCtx(FhirContext ctx) {
		this.ctx = ctx;
	}

	@Override
	public IBaseResource create(IBaseResource fhirResource, Class<? extends Resource> fhirClass) throws SQLException {
		IBaseResource retVal = null;

		IParser parser = ctx.newJsonParser();
		String serialized = parser.encodeResourceToString(fhirResource).replace("'", "''");

		Connection connection = databaseConfiguration.getDataSource().getConnection();

		String query = "SELECT fhirbase_create('"+serialized+"'::jsonb);";
		logger.debug("Query to create: " + query);
		PreparedStatement stmt = connection.prepareStatement(query);

		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			String createdResource = rs.getString("fhirbase_create");
			retVal = parser.parseResource(fhirClass, createdResource);
//			
//			JsonObject jsonObject = new JsonParser().parse(createdResource).getAsJsonObject();
//			JsonElement idValue = jsonObject.get("id");
//			retVal = idValue.getAsString();
		}

		connection.close();

		return retVal;
	}

	@Override
	public IBaseResource read(IdType id, Class<? extends Resource> fhirClass, String tableName) throws SQLException {
		IBaseResource retVal = null;

		Connection connection = databaseConfiguration.getDataSource().getConnection();
		String query = "SELECT resource FROM " + tableName + " where id = ? limit 1";

		logger.debug("Query to read: "+query);
		String idString = id.getIdPart();
		PreparedStatement stmt = connection.prepareStatement(query);
		stmt.setString(1, idString);

		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			String resource = rs.getString("resource");
			IParser parser = ctx.newJsonParser();
			retVal = parser.parseResource(fhirClass, resource);
		}

		connection.close();
		
		return retVal;
	}

	@Override
	public IBaseResource update(IBaseResource fhirResource, Class<? extends Resource> fhirClass) throws SQLException {
		IBaseResource retVal = null;

		IParser parser = ctx.newJsonParser();
		String serialized = parser.encodeResourceToString(fhirResource).replace("'", "''");

		Connection connection = databaseConfiguration.getDataSource().getConnection();

		String query = "SELECT fhirbase_create('"+serialized+"'::jsonb);";
		PreparedStatement stmt = connection.prepareStatement(query);

		logger.debug("Query to update:"+query);

		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			String updatedResource = rs.getString("fhirbase_create");
			if (updatedResource == null || updatedResource.isEmpty()) {
				connection.close();
				throw ThrowFHIRExceptions
						.internalErrorException("Not Existing Resource or Incorrect Resource Content for Update");
			}

			System.out.println("<<<<<<<<<<<<<"+updatedResource);
			retVal = parser.parseResource(fhirClass, updatedResource);
		}

		connection.close();

		return retVal;
	};

	@Override
	public IBaseResource delete(IdType id, Class<? extends Resource> fhirClass, String tableName) throws SQLException {
		IBaseResource retVal = null;
		IParser parser = ctx.newJsonParser();

		String idString = id.getIdPart();

		Connection connection = databaseConfiguration.getDataSource().getConnection();

		String query = "SELECT fhirbase_delete('" + tableName + "', '" + idString + "');";
		PreparedStatement stmt = connection.prepareStatement(query);

		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			String deletedResource = rs.getString("fhirbase_delete");
			if (deletedResource == null || deletedResource.isEmpty()) {
				connection.close();
				throw ThrowFHIRExceptions
						.internalErrorException("Not Existing Resource or Incorrect Resource Content for Delete");
			}

			retVal = parser.parseResource(fhirClass, deletedResource);
		}

		connection.close();

		return retVal;
	}

	@Override
	public List<IBaseResource> search(String sql, Class<? extends Resource> fhirClass) throws SQLException {
		List<IBaseResource> retVal = new ArrayList<IBaseResource>();
		IParser parser = ctx.newJsonParser();

		Connection connection = databaseConfiguration.getDataSource().getConnection();

		PreparedStatement stmt = connection.prepareStatement(sql);

		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			String resourceString = rs.getString("resource");
			if (resourceString == null || resourceString.isEmpty()) {
				connection.close();
				throw ThrowFHIRExceptions.internalErrorException("Empty resource body for search (2)");
			}

			Resource resource = parser.parseResource(fhirClass, resourceString);
			retVal.add(resource);
		}

		connection.close();

		return retVal;
	}

	@Override
	public int getSize(String sql) throws SQLException {
		logger.debug("getSize(): " + sql);
		
		int retVal = 0;
		
		if (databaseConfiguration == null) {
			System.out.println("databaseConfiguration is null");
		}
		Connection connection = databaseConfiguration.getDataSource().getConnection();
		PreparedStatement stmt = connection.prepareStatement(sql);

		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			retVal = rs.getInt("count");
		}

		connection.close();

		return retVal;
	}

}
