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

import jakarta.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Procedure;
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
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Sort;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.ReferenceAndListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class ProcedureResourceProvider extends BaseResourceProvider {

	public ProcedureResourceProvider(FhirContext ctx) {
		super(ctx);

		setTableName(ProcedureResourceProvider.getType().toLowerCase());
		setMyResourceType(ProcedureResourceProvider.getType());
	}

	@PostConstruct
	private void postConstruct() {
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	@Override
	public Class<Procedure> getResourceType() {
		return Procedure.class;
	}

	public static String getType() {
		return "Procedure";
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createProcedure(@ResourceParam Procedure theProcedure) {
		validateResource(theProcedure);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdProcedure = getFhirbaseMapping().create(theProcedure, getResourceType());
			retVal.setId(createdProcedure.getIdElement());
			retVal.setResource(createdProcedure);
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
	 * The "@Update" annotation indicates that this method supports replacing an
	 * existing resource (by ID) with a new instance of that resource.
	 * 
	 * @param theId        This is the ID of the patient to update
	 * @param theProcedure This is the actual resource to save
	 * @return This method returns a "MethodOutcome"
	 */
	@Update()
	public MethodOutcome updateProcedure(@IdParam IdType theId, @ResourceParam Procedure theProcedure) {
		validateResource(theProcedure);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedProcedure = getFhirbaseMapping().update(theProcedure, getResourceType());
			retVal.setId(updatedProcedure.getIdElement());
			retVal.setResource(updatedProcedure);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Delete()
	public void deleteProcedure(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

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
	public IBaseResource readProcedure(@IdParam IdType theId) {
		IBaseResource retVal = null;
				
		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Search()
	public IBundleProvider findProcedureById(
			@RequiredParam(name = Procedure.SP_RES_ID) TokenOrListParam theProcedureIds, @IncludeParam(allow = {
					"Procedure:patient", "Procedure:performer", "Procedure:context" }) final Set<Include> theIncludes) {

		if (theProcedureIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theProcedureId : theProcedureIds.getValuesAsQueryTokens()) {
			whereStatement += "proc.id = '" + theProcedureId.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM procedure proc " + whereStatement;
		String query = "SELECT * FROM procedure proc " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
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
	public IBundleProvider findProceduresByParams(@OptionalParam(name = Procedure.SP_CODE) TokenOrListParam theOrCodes,
			@OptionalParam(name = Procedure.SP_IDENTIFIER) TokenParam theProcecureIdentifier,
			@OptionalParam(name = Procedure.SP_DATE) DateParam theDateParam,
			@OptionalParam(name = Procedure.SP_ENCOUNTER) ReferenceParam theEncounterParam,
			@OptionalParam(name = Procedure.SP_SUBJECT, chainWhitelist = { "", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceAndListParam theSubjects,
			@OptionalParam(name = Procedure.SP_PATIENT, chainWhitelist = { "", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceAndListParam thePatients,
			@OptionalParam(name = Procedure.SP_PERFORMER) ReferenceParam thePerformerParam, @Sort SortSpec theSort,

			@IncludeParam(allow = { "Procedure:patient", "Procedure:performer",
					"Procedure:context" }) final Set<Include> theIncludes) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = "procedure proc";

		if (theSubjects != null || thePatients != null) {
			fromStatement += " join patient p on proc.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";

			String updatedFromStatement = constructFromWherePatients (fromStatement, whereParameters, theSubjects);
			if (updatedFromStatement.isEmpty()) {
				// This means that we have unsupported resource. Since this is to search, we should discard all and
				// return null.
				return null;
			}
			fromStatement = updatedFromStatement;

			updatedFromStatement = constructFromWherePatients(fromStatement, whereParameters, thePatients);
			if (updatedFromStatement.isEmpty()) {
				// This means that we have unsupported resource. Since this is to search, we should discard all and
				// return null.
				return null;
			}
			fromStatement = updatedFromStatement;
			
			returnAll = false;
		}
		
		if (theOrCodes != null) {
			fromStatement = constructFromStatementPath(fromStatement, "codings", "proc.resource->'code'->'coding'");
			String where = constructCodeWhereParameter(theOrCodes);
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
			returnAll = false;
		}

		if (theProcecureIdentifier != null) {
			String system = theProcecureIdentifier.getSystem();
			String value = theProcecureIdentifier.getValue();

			if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
				whereParameters.add("proc.resource->'identifier' @> '[{\"value\": \"" + value + "\",\"system\": \""
						+ system + "\"}]'::jsonb");
			} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
				whereParameters.add("proc.resource->'identifier' @> '[{\"system\": \"" + system + "\"}]'::jsonb");
			} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
				whereParameters.add("proc.resource->'identifier' @> '[{\"value\": \"" + value + "\"}]'::jsonb");
			}
			returnAll = false;
		}

		if (theDateParam != null) {
			String where = constructDateWhereParameter(theDateParam, "proc", "performedDateTime");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
			returnAll = false;
		}
		if (theEncounterParam != null) {
			whereParameters.add("proc.resource->>'encounter' like '%" + theEncounterParam.getValue() + "%'");
			returnAll = false;
		}

		if (thePerformerParam != null) {
			whereParameters.add("proc.resource->>'performer' like '%" + theEncounterParam.getValue() + "%'");
			returnAll = false;
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll && (whereStatement == null || whereStatement.isEmpty())) {
			return null;
		}

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	/**
	 * This method just provides simple business validation for resources we are
	 * storing.
	 * 
	 * @param thePractitioner The thePractitioner to validate
	 */
	private void validateResource(Procedure theProcedure) {
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
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();
			
			// _Include
			List<String> includes = new ArrayList<String>();
			if (theIncludes.contains(new Include("Procedure:patient"))) {
				includes.add("Procedure:patient");
			}

			if (theIncludes.contains(new Include("Procedure:performer"))) {
				includes.add("Procedure:performer");
			}

			if (theIncludes.contains(new Include("Procedure:context"))) {
				includes.add("Procedure:context");
			}

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
