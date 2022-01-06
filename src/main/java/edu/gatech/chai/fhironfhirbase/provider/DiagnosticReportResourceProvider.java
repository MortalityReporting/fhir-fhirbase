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
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.DocumentReference;
import org.hl7.fhir.r4.model.IdType;
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
public class DiagnosticReportResourceProvider extends BaseResourceProvider {

	public DiagnosticReportResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(DiagnosticReportResourceProvider.getType().toLowerCase());
		setMyResourceType(DiagnosticReportResourceProvider.getType());

		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	public static String getType() {
		return "DiagnosticReport";
	}

	@Override
	public Class<DiagnosticReport> getResourceType() {
		return DiagnosticReport.class;
	}

	@Create()
	public MethodOutcome createDiagnosticReport(@ResourceParam DiagnosticReport theDiagnosticReport) {
		validateResource(theDiagnosticReport);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdDiagnosticReport = getFhirbaseMapping().create(theDiagnosticReport, getResourceType());
			retVal.setId(createdDiagnosticReport.getIdElement());
			retVal.setResource(createdDiagnosticReport);
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
	public void deleteDiagnosticReport(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	@Read()
	public IBaseResource readDiagnosticReport(@IdParam IdType theId) {
		IBaseResource retVal = null;
		
		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Update()
	public MethodOutcome updateDiagnosticReport(@IdParam IdType theId,
			@ResourceParam DiagnosticReport theDiagnosticReport) {
		validateResource(theDiagnosticReport);

		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedDiagnosticReport = getFhirbaseMapping().update(theDiagnosticReport, getResourceType());
			retVal.setId(updatedDiagnosticReport.getIdElement());
			retVal.setResource(updatedDiagnosticReport);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Search()
	public IBundleProvider findDiagnosticReportByIds(
			@RequiredParam(name = DiagnosticReport.SP_RES_ID) TokenOrListParam theDiagnosticReportIds,

			@IncludeParam(allow = { "DiagnosticReport:patient", "DiagnosticReport:subject",
					"DiagnosticReport:encounter" }) final Set<Include> theIncludes,

			@IncludeParam(reverse = true) final Set<Include> theReverseIncludes) {

		if (theDiagnosticReportIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theDiagnosticReport : theDiagnosticReportIds.getValuesAsQueryTokens()) {
			whereStatement += "diag.id = '" + theDiagnosticReport.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM documentreference diag " + whereStatement;
		String query = "SELECT * FROM documentreference diag " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, theReverseIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findDiagnosticReportsByParams(
			@OptionalParam(name = DiagnosticReport.SP_PATIENT, chainWhitelist = { "",
					USCorePatient.SP_NAME }) ReferenceAndListParam thePatients,
			@OptionalParam(name = DiagnosticReport.SP_SUBJECT, chainWhitelist = { "",
					USCorePatient.SP_NAME }) ReferenceAndListParam theSubjects,
			@OptionalParam(name = DiagnosticReport.SP_IDENTIFIER) TokenParam theDocumentReferenceIdentifier,
			@OptionalParam(name = DiagnosticReport.SP_ENCOUNTER) ReferenceParam theEncounter,
			@Sort SortSpec theSort,
			@IncludeParam(allow = { "DiagnosticReport:patient", "DiagnosticReport:subject",
					"DiagnosticReport:encounter" }) final Set<Include> theIncludes,

			@IncludeParam(reverse = true) final Set<Include> theReverseIncludes) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = getTableName() + " diag";

		if (theSubjects != null || thePatients != null) {
			fromStatement += " join patient p on diag.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";

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

		if (theEncounter != null) {
			whereParameters.add("diag.resource->'context'->>'encounter' like '%" + theEncounter.getValue() + "%'");
			returnAll = false;
		}

		if (theDocumentReferenceIdentifier != null) {
			String system = theDocumentReferenceIdentifier.getSystem();
			String value = theDocumentReferenceIdentifier.getValue();

			if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
				whereParameters.add("diag.resource->'identifier' @> '[{\"value\": \"" + value + "\",\"system\": \""
						+ system + "\"}]'::jsonb");
			} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
				whereParameters.add("diag.resource->'identifier' @> '[{\"system\": \"" + system + "\"}]'::jsonb");
			} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
				whereParameters.add("diag.resource->'identifier' @> '[{\"value\": \"" + value + "\"}]'::jsonb");
			}
			returnAll = false;
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll && (whereStatement == null || whereStatement.isEmpty())) { 
			return null;
		}

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, theReverseIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
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

			if (theIncludes.contains(new Include("DiagnosticReport:encounter"))) {
				includes.add("DiagnosticReport:encounter");
			}

			if (theIncludes.contains(new Include("DiagnosticReport:patient"))) {
				includes.add("DiagnosticReport:patient");
			}

			if (theIncludes.contains(new Include("DiagnosticReport:subject"))) {
				includes.add("DiagnosticReport:subject");
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

	/* TODO: 
	 * Add more validation code here.
	 */
	private void validateResource(DiagnosticReport theDiagnosticReport) {
		
	}

}
