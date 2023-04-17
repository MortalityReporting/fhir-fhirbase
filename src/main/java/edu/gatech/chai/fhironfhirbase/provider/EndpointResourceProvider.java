/*
 * Filename: /Users/mc142/Documents/workspace/mortality-reporting/raven-fhir-server-dev/fhir-fhirbase/src/main/java/edu/gatech/chai/fhironfhirbase/provider/LocationResourceProvider copy.java
 * Path: /Users/mc142/Documents/workspace/mortality-reporting/raven-fhir-server-dev/fhir-fhirbase/src/main/java/edu/gatech/chai/fhironfhirbase/provider
 * Created Date: Friday, April 14th 2023, 5:37:04 pm
 * Author: Myung Choi
 * 
 * Copyright (c) 2023 GTRI - Health Emerging and Advanced Technologies (HEAT)
 */
package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Endpoint;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Sort;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.operation.FhirbaseMapping;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class EndpointResourceProvider extends BaseResourceProvider {
	protected FhirbaseMapping fhirbaseMapping;

	public EndpointResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(EndpointResourceProvider.getType().toLowerCase());
		setMyResourceType(EndpointResourceProvider.getType());

		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	public static String getType() {
		return "Endpoint";
	}

	@Override
	public Class<Endpoint> getResourceType() {
		return Endpoint.class;
	}

	@Create()
	public MethodOutcome createEndpoint(@ResourceParam Endpoint endpoint) {
		validateResource(endpoint);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdEndpoint = getFhirbaseMapping().create(endpoint, getResourceType());
			retVal.setId(createdEndpoint.getIdElement());
			retVal.setResource(createdEndpoint);
			retVal.setCreated(true);
		} catch (SQLException e) {
			retVal.setCreated(false);
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

		return retVal;
	}

	@Delete()
	public void deleteLocation(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	@Read()
	public IBaseResource readEndpoint(@IdParam IdType theId) {
		IBaseResource retVal = null;
		
		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Update()
	public MethodOutcome updateEndpoint(@IdParam IdType theId, @ResourceParam Endpoint endpoint) {
		validateResource(endpoint);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedEndpoint = getFhirbaseMapping().update(endpoint, getResourceType());
			retVal.setId(updatedEndpoint.getIdElement());
			retVal.setResource(updatedEndpoint);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Search()
	public IBundleProvider findEndpointByIds(
			@RequiredParam(name = Endpoint.SP_RES_ID) TokenOrListParam theEndpointIds) {
		if (theEndpointIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theEndpointIds.getValuesAsQueryTokens()) {
			whereStatement += "lo.id = '" + theCondition.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM location lo " + whereStatement;
		String query = "SELECT * FROM location lo " + whereStatement;
		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findEndpointByParams(@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = "endpoint e";

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll) {
			if (whereStatement == null || whereStatement.isEmpty()) return null;
		}

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	// TODO: Add more validation code here.
	private void validateResource(Endpoint endpoint) {
	}

	class MyBundleProvider extends FhirbaseBundleProvider {
		public MyBundleProvider(String query) {
			super(query);
			setPreferredPageSize(preferredPageSize);
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();
			
			// _Include
			// TODO: do this later
			List<String> includes = new ArrayList<String>();

			String myQuery = query;			
			if (toIndex - fromIndex > 0) {
				myQuery += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			try {
				retVal.addAll(getFhirbaseMapping().search(myQuery, getResourceType()));
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			return retVal;
		}

	}

}
