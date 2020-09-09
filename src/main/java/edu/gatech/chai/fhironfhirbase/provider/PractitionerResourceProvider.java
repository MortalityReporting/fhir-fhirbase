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
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Practitioner;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.IncludeParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Sort;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;

/**
 * This is a resource provider which stores Patient resources in memory using a
 * HashMap. This is obviously not a production-ready solution for many reasons,
 * but it is useful to help illustrate how to build a fully-functional server.
 */
@Service
@Scope("prototype")
public class PractitionerResourceProvider extends BaseResourceProvider {

	private int preferredPageSize = 30;

	public PractitionerResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(PractitionerResourceProvider.getType().toLowerCase());
		setMyResourceType(PractitionerResourceProvider.getType());
		getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
	}

	public static String getType() {
		return "Practitioner";
	}

	/**
	 * The getResourceType method comes from IResourceProvider, and must be
	 * overridden to indicate what type of resource this provider supplies.
	 */
	@Override
	public Class<Practitioner> getResourceType() {
		return Practitioner.class;
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createPractitioner(@ResourceParam Practitioner thePractitioner) {
		validateResource(thePractitioner);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdPractitioner = getFhirbaseMapping().create(thePractitioner, getResourceType());
			retVal.setId(createdPractitioner.getIdElement());
			retVal.setResource(createdPractitioner);
			retVal.setCreated(true);
		} catch (SQLException e) {
			retVal.setCreated(false);
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Delete()
	public void deletePractitioner(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	@Search()
	public IBundleProvider findPractitionersByParams(
			@OptionalParam(name = Practitioner.SP_RES_ID) TokenOrListParam thePractitionerIds,
			@OptionalParam(name = Practitioner.SP_IDENTIFIER) TokenParam thePractitionerIdentifier,
			@OptionalParam(name = Practitioner.SP_ACTIVE) TokenParam theActive,
			@OptionalParam(name = Practitioner.SP_FAMILY) StringParam theFamilyName,
			@OptionalParam(name = Practitioner.SP_GIVEN) StringParam theGivenName,
			@OptionalParam(name = Practitioner.SP_NAME) StringParam theName,
			@OptionalParam(name = Practitioner.SP_GENDER) StringParam theGender, @Sort SortSpec theSort,
			@IncludeParam(allow = {}) final Set<Include> theIncludes,
			@IncludeParam(reverse = true) final Set<Include> theReverseIncludes) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = "practitioner pract";

		if (thePractitionerIds != null) {
			for (TokenParam thePractitionerId : thePractitionerIds.getValuesAsQueryTokens()) {
				whereParameters.add("pract.id = " + thePractitionerId.getValue());
			}
			returnAll = false;
		}
		
		if (thePractitionerIdentifier != null) {
			String system = thePractitionerIdentifier.getSystem();
			String value = thePractitionerIdentifier.getValue();

			if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
				whereParameters.add("pract.resource->'identifier' @> '[{\"value\": \"" + value + "\",\"system\": \""
						+ system + "\"}]'::jsonb");
			} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
				whereParameters.add("pract.resource->'identifier' @> '[{\"system\": \"" + system + "\"}]'::jsonb");
			} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
				whereParameters.add("pract.resource->'identifier' @> '[{\"value\": \"" + value + "\"}]'::jsonb");
			}
			returnAll = false;
		}

		if (theActive != null) {
			whereParameters.add("pract.resource->>'active'=" + theActive.getValue());
			returnAll = false;
		}

		if (theFamilyName != null) {
			if (!fromStatement.contains("names")) {
				fromStatement += ", jsonb_array_elements(pract.resource->'name') names";
			}

			if (theFamilyName.isExact()) {
				whereParameters.add("names->>'family' = '" + theFamilyName.getValue() + "'");
			} else {
				whereParameters.add("names->>'family' like '%" + theFamilyName.getValue() + "%'");
			}
			returnAll = false;
		}

		if (theGivenName != null) {
			if (!fromStatement.contains("names")) {
				fromStatement += ", jsonb_array_elements(pract.resource->'name') names";
			}

			whereParameters.add("names->>'given' like '%" + theGivenName.getValue() + "%'");
			returnAll = false;
		}
		if (theName != null) {
			whereParameters.add("pract.resource->>'name' like '%" + theName.getValue() + "%'");
			returnAll = false;
		}

		if (theGender != null) {
			whereParameters.add("pract.resource->>'gender' = '" + theGender.getValue() + "'");
			returnAll = false;
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll) {
			if (whereStatement == null || whereStatement.isEmpty()) return null;
		}

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, theReverseIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;

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
	public IBaseResource readPractitioner(@IdParam IdType theId) {
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
	 * @param theId           This is the ID of the patient to update
	 * @param thePractitioner This is the actual resource to save
	 * @return This method returns a "MethodOutcome"
	 */
	@Update()
	public MethodOutcome updatePractitioner(@IdParam IdType theId, @ResourceParam Practitioner thePractitioner) {
		validateResource(thePractitioner);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedPractitioner = getFhirbaseMapping().update(thePractitioner, getResourceType());
			retVal.setId(updatedPractitioner.getIdElement());
			retVal.setResource(updatedPractitioner);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	/**
	 * This method just provides simple business validation for resources we are
	 * storing.
	 * 
	 * @param thePractitioner The thePractitioner to validate
	 */
	private void validateResource(Practitioner thePractitioner) {
		/*
		 * Our server will have a rule that practitioners must have a name or we will
		 * reject them
		 */
		if (thePractitioner.getName().isEmpty()) {
			ThrowFHIRExceptions.unprocessableEntityException(
					"No name provided, Practictioner resources must have at least one name.");
		}
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
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
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();
			
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