package edu.gatech.chai.fhironfhirbase.operation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import edu.gatech.chai.fhironfhirbase.database.DatabaseConfiguration;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;


@Component
public class FhirbaseMapping implements IResourceMapping {

	@Autowired
	DatabaseConfiguration databaseConfiguration;

	protected FhirContext ctx;
	protected Class<? extends Resource> fhirClass;
	protected String tableName;

//	public FhirbaseMapping() {
//	}
//	
//	public FhirbaseMapping(Class<? extends Resource> fhirClass, String tableName) {
//		ctx = FhirContext.forR4();
//
//		setFhirClass(fhirClass);
//		setTableName(tableName);
//	}

	public Class<? extends Resource> getFhirClass() {
		return fhirClass;
	}

	public void setFhirClass(Class<? extends Resource> fhirClass) {
		this.fhirClass = fhirClass;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public FhirContext getCtx() {
		return this.ctx;
	}
	
	public void setCtx(FhirContext ctx) {
		this.ctx = ctx;
	}

	@Override
	public String create(IBaseResource fhirResource) throws Exception {
		String retVal = null;

		IParser parser = ctx.newJsonParser();
		String serialized = parser.encodeResourceToString(fhirResource).replace("'", "''");

		Connection connection = databaseConfiguration.getDataSource().getConnection();

		String query = "SELECT fhirbase_create('"+serialized+"'::jsonb);";
		System.out.println("\n\n\n"+query+"\n\n\n");
		PreparedStatement stmt = connection.prepareStatement(query);

		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			String createdResource = rs.getString("fhirbase_create");
			JsonObject jsonObject = new JsonParser().parse(createdResource).getAsJsonObject();
			JsonElement idValue = jsonObject.get("id");
			retVal = idValue.getAsString();
		}

		connection.close();

		return retVal;
	}

	@Override
	public IBaseResource read(IdType id, Class<? extends Resource> fhirClass, String tableName) throws Exception {
		IBaseResource retVal = null;

		Connection connection = databaseConfiguration.getDataSource().getConnection();
		String query = "SELECT resource FROM " + tableName + " where id = ? limit 1";

		System.out.println("READ: "+query);
		String idString = id.getIdPart();
		PreparedStatement stmt = connection.prepareStatement(query);
		stmt.setString(1, idString);

		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			String resource = rs.getString("resource");
			System.out.println("READ RESOURCE: "+resource);
			IParser parser = ctx.newJsonParser();
			retVal = parser.parseResource(fhirClass, resource);
		}

		return retVal;
	}

	@Override
	public IBaseResource update(IBaseResource fhirResource, Class<? extends Resource> fhirClass) throws Exception {
		IBaseResource retVal = null;

		IParser parser = ctx.newJsonParser();
		String serialized = parser.encodeResourceToString(fhirResource).replace("'", "''");

		Connection connection = databaseConfiguration.getDataSource().getConnection();

		String query = "SELECT fhirbase_create('"+serialized+"'::jsonb);";
		PreparedStatement stmt = connection.prepareStatement(query);

		System.out.println("<<<<<<<<<<<<< SQL:"+query);

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
	public IBaseResource delete(IdType id) throws Exception {
		IBaseResource retVal = null;
		IParser parser = ctx.newJsonParser();

		String idString = id.getIdPart();

		Connection connection = databaseConfiguration.getDataSource().getConnection();

		String query = "SELECT fhirbase_delete('" + getTableName() + "', '" + idString + "');";
		PreparedStatement stmt = connection.prepareStatement(query);

		ResultSet rs = stmt.executeQuery();
		if (rs.next()) {
			String deletedResource = rs.getString("fhirbase_delete");
			if (deletedResource == null || deletedResource.isEmpty()) {
				connection.close();
				throw ThrowFHIRExceptions
						.internalErrorException("Not Existing Resource or Incorrect Resource Content for Delete");
			}

			retVal = parser.parseResource(getFhirClass(), deletedResource);
		}

		connection.close();

		return retVal;
	}

	@Override
	public List<IBaseResource> search(String sql, Class<? extends Resource> fhirClass) throws Exception {
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
	public int getSize(String sql) throws Exception {
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

	@Override
	public IBaseResource delete(String tableName, String id, Class<? extends Resource> fhirClass) throws Exception {
		IBaseResource retVal = null;
		IParser parser = ctx.newJsonParser();

		Connection connection = databaseConfiguration.getDataSource().getConnection();

		String query = "SELECT fhirbase_delete('" + tableName + "', '" + id + "');";
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

}
