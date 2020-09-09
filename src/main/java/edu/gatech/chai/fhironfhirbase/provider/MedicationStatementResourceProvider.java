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

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.MedicationStatement;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
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
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;

@Service
@Scope("prototype")
public class MedicationStatementResourceProvider extends BaseResourceProvider {

	private int preferredPageSize = 30;

	public MedicationStatementResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(MedicationStatementResourceProvider.getType().toLowerCase());
		setMyResourceType(MedicationStatementResourceProvider.getType());
		getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
	}

	@Override
	public Class<MedicationStatement> getResourceType() {
		return MedicationStatement.class;
	}

	public static String getType() {
		return "MedicationStatement";
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createMedicationStatement(@ResourceParam MedicationStatement theMedicationStatement) {
		validateResource(theMedicationStatement);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource createdMedStatement = getFhirbaseMapping().create(theMedicationStatement, getResourceType());
			retVal.setId(createdMedStatement.getIdElement());
			retVal.setResource(createdMedStatement);
			retVal.setCreated(true);
		} catch (SQLException e) {
			retVal.setCreated(false);
			e.printStackTrace();
		}

		return retVal;
	}

	@Delete()
	public void deleteMedicationStatement(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Update()
	public MethodOutcome updateMedicationStatement(@IdParam IdType theId,
			@ResourceParam MedicationStatement theMedicationStatement) {
		validateResource(theMedicationStatement);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource updatedMedStatement = getFhirbaseMapping().update(theMedicationStatement, getResourceType());
			retVal.setId(updatedMedStatement.getIdElement());
			retVal.setResource(updatedMedStatement);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	@Read()
	public IBaseResource readMedicationStatement(@IdParam IdType theId) {
		IBaseResource retVal = null;

		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	@Search()
	public IBundleProvider findMedicationStatementsById(
			@RequiredParam(name = MedicationStatement.SP_RES_ID) TokenOrListParam theMedicationStatementIds,
			@Sort SortSpec theSort) {

		if (theMedicationStatementIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theMedicationStatement : theMedicationStatementIds.getValuesAsQueryTokens()) {
			whereStatement += "ms.id = " + theMedicationStatement.getValue() + " OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM medicationstatement ms " + whereStatement;
		String query = "SELECT * FROM medicationstatement ms " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findMedicationStatementsByParams(
			@OptionalParam(name = MedicationStatement.SP_CODE) TokenOrListParam theOrCodes,
			@OptionalParam(name = MedicationStatement.SP_CONTEXT) ReferenceParam theContext,
			@OptionalParam(name = MedicationStatement.SP_EFFECTIVE) DateParam theDate,
			@OptionalParam(name = MedicationStatement.SP_PATIENT, chainWhitelist = { "", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceOrListParam thePatients,
			@OptionalParam(name = MedicationStatement.SP_SUBJECT, chainWhitelist = { "", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceOrListParam theSubjects,
			@OptionalParam(name = MedicationStatement.SP_SOURCE) ReferenceParam theSource, @Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = "medicationstatement ms";
		if (theOrCodes != null) {
			fromStatement = constructFromStatementPath(fromStatement, "codes",
					"ms.resource->'medicationCodeableConcept'->'coding'");
			String where = constructCodeWhereParameter(theOrCodes);
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
			returnAll = false;
		}
		if (theContext != null) {
			whereParameters.add("ms.resource->context->>reference like '%" + theContext.getValue() + "%'");
			returnAll = false;
		}

		if (theDate != null) {
			String where = constructDateWhereParameter(theDate, "ms", "effectiveDateTime");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
			returnAll = false;
		}

		if (theSource != null) {
			whereParameters.add("ms.resource->informationSource->>reference like '%" + theSource.getValue() + "%'");
			returnAll = false;
		}

		if (theSubjects != null) {
			for (ReferenceParam theSubject : theSubjects.getValuesAsQueryTokens()) {
				String where = constructSubjectWhereParameter(theSubject, "ms");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
			returnAll = false;
		}

		if (thePatients != null) {
			for (ReferenceParam thePatient : thePatients.getValuesAsQueryTokens()) {
				String where = constructPatientWhereParameter(thePatient, "ms");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
			returnAll = false;
		}

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

	private void errorProcessing(String msg) {
		OperationOutcome outcome = new OperationOutcome();
		CodeableConcept detailCode = new CodeableConcept();
		detailCode.setText(msg);
		outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
		throw new UnprocessableEntityException(FhirContext.forDstu3(), outcome);
	}

	/**
	 * This method just provides simple business validation for resources we are
	 * storing.
	 * 
	 * @param theMedication The medication statement to validate
	 */
	private void validateResource(MedicationStatement theMedication) {
		/*
		 * Our server will have a rule that patients must have a family name or we will
		 * reject them
		 */
//		if (thePatient.getNameFirstRep().getFamily().isEmpty()) {
//			OperationOutcome outcome = new OperationOutcome();
//			CodeableConcept detailCode = new CodeableConcept();
//			detailCode.setText("No family name provided, Patient resources must have at least one family name.");
//			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
//			throw new UnprocessableEntityException(FhirContext.forDstu3(), outcome);
//		}
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {

		public MyBundleProvider(String query) {
			super(query);
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();
			// _Include
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
