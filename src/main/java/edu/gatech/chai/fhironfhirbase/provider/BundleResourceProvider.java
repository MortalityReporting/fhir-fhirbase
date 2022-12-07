/*
 * Filename: /Users/mc142/Documents/workspace/mortality-reporting/raven-fhir-server-dev/fhir-fhirbase/src/main/java/edu/gatech/chai/fhironfhirbase/provider/PatientResourceProvider copy.java
 * Path: /Users/mc142/Documents/workspace/mortality-reporting/raven-fhir-server-dev/fhir-fhirbase/src/main/java/edu/gatech/chai/fhironfhirbase/provider
 * Created Date: Saturday, December 3rd 2022, 5:26:07 pm
 * Author: Myung Choi
 * 
 * Copyright (c) 2022 GTRI - Health Emerging and Advanced Technologies (HEAT)
 */
/*******************************************************************************
 * Copyright (c) 2019 Georgia Tech Research Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.codesystems.MeasureScoring;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Sort;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

/**
 * This is a resource provider which stores Patient resources in memory using a
 * HashMap. This is obviously not a production-ready solution for many reasons,
 * but it is useful to help illustrate how to build a fully-functional server.
 */
@Service
@Scope("prototype")
public class BundleResourceProvider extends BaseResourceProvider {

	public BundleResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(BundleResourceProvider.getType().toLowerCase());
		setMyResourceType(BundleResourceProvider.getType());

		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	public static String getType() {
		return "Bundle";
	}

	/**
	 * The getResourceType method comes from IResourceProvider, and must be
	 * overridden to indicate what type of resource this provider supplies.
	 */
	@Override
	public Class<Bundle> getResourceType() {
		return Bundle.class;
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createBundle(@ResourceParam Bundle theBundle) {
		validateResource(theBundle);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource createdBundle = getFhirbaseMapping().create(theBundle, getResourceType());
			retVal.setId(createdBundle.getIdElement());
			retVal.setResource(createdBundle);
			retVal.setCreated(true);
		} catch (SQLException e) {
			retVal.setCreated(false);
			e.printStackTrace();
		}

		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

		return retVal;
	}

	/**
	 * This is the "read" operation. The "@Read" annotation indicates that this
	 * method supports the read and/or vread operation.
	 * <p>
	 * Read operations take a single parameter annotated with the {@link IdParam}
	 * paramater, and should return a single resource instance.
	 * </p>
	 * 
	 * @param theId The read operation takes one parameter, which must be of type
	 *              IdDt and must be annotated with the "@Read.IdParam" annotation.
	 * @return Returns a resource matching this identifier, or null if none exists.
	 */
	@Read()
	public IBaseResource readBundle(@IdParam IdType theId) {
		IBaseResource retVal = null;

		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	/**
	 * The "@Update" annotation indicates that this method supports replacing an
	 * existing resource (by ID) with a new instance of that resource.
	 * 
	 * @param theId      This is the ID of the patient to update
	 * @param thePatient This is the actual resource to save
	 * @return This method returns a "MethodOutcome"
	 */
	@Update()
	public MethodOutcome updateBundle(@IdParam IdType theId, @ResourceParam Bundle theBundle) {
		validateResource(theBundle);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource updatedBundle = getFhirbaseMapping().update(theBundle, getResourceType());
			retVal.setId(updatedBundle.getIdElement());
			retVal.setResource(updatedBundle);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	@Delete()
	public void deleteBundle(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	/**
	 * The "@Search" annotation indicates that this method supports the search
	 * operation. You may have many different method annotated with this annotation,
	 * to support many different search criteria. This example searches by family
	 * name.
	 * 
	 * @param theFamilyName This operation takes one parameter which is the search
	 *                      criteria. It is annotated with the "@Required"
	 *                      annotation. This annotation takes one argument, a string
	 *                      containing the name of the search criteria. The datatype
	 *                      here is StringParam, but there are other possible
	 *                      parameter types depending on the specific search
	 *                      criteria.
	 * @return This method returns a list of Patients in bundle. This list may
	 *         contain multiple matching resources, or it may also be empty.
	 */
	@Search(allowUnknownParams = true)
	public IBundleProvider findBundlesByParams(RequestDetails theRequestDetails,
			@OptionalParam(name = Bundle.SP_RES_ID) TokenParam theBundleId,
			@OptionalParam(name = Bundle.SP_IDENTIFIER) TokenParam theBundleIdentifier,
			@OptionalParam(name = Bundle.SP_MESSAGE) ReferenceOrListParam theMessages,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = "bundle b";

		if (theBundleId != null) {
			whereParameters.add("b.id = '" + theBundleId.getValue() + "'");
			returnAll = false;
		}

		if (theBundleIdentifier != null) {
			String system = theBundleIdentifier.getSystem();
			String value = theBundleIdentifier.getValue();

			if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
				whereParameters.add("b.resource->'identifier' @> '[{\"value\": \"" + value + "\",\"system\": \""
						+ system + "\"}]'::jsonb");
			} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
				whereParameters.add("b.resource->'identifier' @> '[{\"system\": \"" + system + "\"}]'::jsonb");
			} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
				whereParameters.add("b.resource->'identifier' @> '[{\"value\": \"" + value + "\"}]'::jsonb");
			}
			returnAll = false;
		}

		if (theMessages != null) {
			String messageEntries = "";
			for (ReferenceParam messageReference : theMessages.getValuesAsQueryTokens()) {
				if (!messageEntries.isEmpty()) {
					messageEntries += " OR ";
				}
				String messageId = messageReference.getResourceType() + "/" + messageReference.getIdPart();

				messageEntries += "b.resource->'entry'->0->>'fullUrl' like '%" + messageId + "%'";
			}

			if (!messageEntries.isEmpty()) {
				whereParameters.add("b.resource->>'type' = 'message'");
				whereParameters.add(messageEntries);

				returnAll = false;
			}
		}

		// Complete Query.
		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll && (whereStatement == null || whereStatement.isEmpty())) {
			return null;
		}

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;
		MyBundleProvider myBundleProvider = new MyBundleProvider(query, null, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;

	}

	/**
	 * This method just provides simple business validation for resources we are
	 * storing.
	 * 
	 * @param thePatient The patient to validate
	 */
	private void validateResource(Bundle theBundle) {
		/*
		 * Our server will have a rule that patients must have a family name or we will
		 * reject them
		 * 
		 * commenting out this name validation as VRDR for MDI may not have names.
		 */
//		if (thePatient.getNameFirstRep().getFamily().isEmpty()) {
//			OperationOutcome outcome = new OperationOutcome();
//			CodeableConcept detailCode = new CodeableConcept();
//			detailCode.setText("No family name provided, Patient resources must have at least one family name.");
//			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
//			throw new UnprocessableEntityException(getFhirContext(), outcome);
//		}
	}

	class MyBundleProvider extends FhirbaseBundleProvider {
		Set<Include> theIncludes;
		Set<Include> theReverseIncludes;

		public MyBundleProvider(String query, Set<Include> theIncludes, Set<Include> theReverseIncludes) {
			super(query);
			setPreferredPageSize(preferredPageSize);
			this.theIncludes = theIncludes;
			this.theReverseIncludes = theReverseIncludes;
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			List<IBaseResource> retv = new ArrayList<IBaseResource>();

			String myQuery = query;			
			if (toIndex - fromIndex > 0) {
				myQuery += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			try {
				retv.addAll(getFhirbaseMapping().search(myQuery, getResourceType()));
			} catch (SQLException e) {
				e.printStackTrace();
			}

			return retv;
		}

	}

}