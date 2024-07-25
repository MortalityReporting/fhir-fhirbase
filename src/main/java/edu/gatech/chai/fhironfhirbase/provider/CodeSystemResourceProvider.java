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
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
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
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class CodeSystemResourceProvider extends BaseResourceProvider {

	public CodeSystemResourceProvider(FhirContext ctx) {
		super(ctx);

		setTableName(CodeSystemResourceProvider.getType().toLowerCase());
		setMyResourceType(CodeSystemResourceProvider.getType());
	}

	@PostConstruct
	private void postConstruct() {
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	public static String getType() {
		return "CodeSystem";
	}

	@Override
	public Class<CodeSystem> getResourceType() {
		return CodeSystem.class;
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createCodeSystem(@ResourceParam CodeSystem theCodeSystem) {
		validateResource(theCodeSystem);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdCodeSystem = getFhirbaseMapping().create(theCodeSystem, getResourceType());
			retVal.setId(createdCodeSystem.getIdElement());
			retVal.setResource(createdCodeSystem);
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
	public void deleteCodeSystem (@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	@Search()
	public IBundleProvider findCodeSystemsById(
		@RequiredParam(name = CodeSystem.SP_RES_ID) TokenOrListParam theCodeSystemIds, @Sort SortSpec theSort) {

		if (theCodeSystemIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCodeSystem : theCodeSystemIds.getValuesAsQueryTokens()) {
			whereStatement += "codesys.id = '" + theCodeSystem.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM " + getTableName() + " codesys " + whereStatement;
		String query = "SELECT * FROM " + getTableName() + " codesys " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, null, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findCodeSystemsByParams(
			@OptionalParam(name = CodeSystem.SP_CODE) StringOrListParam theCodes,
			@OptionalParam(name = CodeSystem.SP_DATE) DateParam theDate,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = getTableName() + " codesys";
		
		if (theCodes != null) {
			String codesQuery = new String();
			for (StringParam theCode : theCodes.getValuesAsQueryTokens()) {
				if (codesQuery.isBlank()) {
					codesQuery.concat("codesys.resource->'concept'->>'code' = '" + theCode.getValue() + "'");
				} else {
					codesQuery.concat("OR codesys.resource->'concept'->>'code' = '" + theCode.getValue() + "'");
				}
			}
			whereParameters.add("(" + codesQuery + ")");
			returnAll = false;
		}

		if (theDate != null) {
			String where = constructDateWhereParameter(theDate, "codesys", "date");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
			returnAll = false;
		}

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
	public IBaseResource readCodeSystem(@IdParam IdType theId) {
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
	public MethodOutcome updateCodeSystem(@IdParam IdType theId, @ResourceParam CodeSystem theCodeSystem) {
		validateResource(theCodeSystem);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedCodeSystem = getFhirbaseMapping().update(theCodeSystem, getResourceType());
			retVal.setId(updatedCodeSystem.getIdElement());
			retVal.setResource(updatedCodeSystem);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	private void validateResource(CodeSystem theCodeSystem) {
		OperationOutcome outcome = new OperationOutcome();
		CodeableConcept detailCode = new CodeableConcept();
		if (theCodeSystem.getStatus() == null) {
			detailCode.setText("No status is provided.");
			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}
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
