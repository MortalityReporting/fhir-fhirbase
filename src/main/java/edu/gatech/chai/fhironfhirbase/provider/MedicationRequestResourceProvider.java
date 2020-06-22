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
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Medication;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
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
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;

@Service
public class MedicationRequestResourceProvider extends BaseResourceProvider {

	private int preferredPageSize = 30;

	public MedicationRequestResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(MedicationRequestResourceProvider.getType().toLowerCase());
		setMyResourceType(MedicationRequestResourceProvider.getType());
		getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
	}

	public static String getType() {
		return "MedicationRequest";
	}

	@Override
	public Class<MedicationRequest> getResourceType() {
		return MedicationRequest.class;
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createMedicationRequest(@ResourceParam MedicationRequest theMedicationRequest) {
		validateResource(theMedicationRequest);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource createdMedicationRequest = getFhirbaseMapping().create(theMedicationRequest,
					getResourceType());
			retVal.setId(createdMedicationRequest.getIdElement());
			retVal.setResource(createdMedicationRequest);
			retVal.setCreated(true);
		} catch (SQLException e) {
			retVal.setCreated(false);
			e.printStackTrace();
		}

		return retVal;
	}

	@Delete()
	public void deleteMedicationRequest(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Update()
	public MethodOutcome updateMedicationRequest(@IdParam IdType theId,
			@ResourceParam MedicationRequest theMedicationRequest) {
		validateResource(theMedicationRequest);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource updatedMedicationRequest = getFhirbaseMapping().update(theMedicationRequest,
					getResourceType());
			retVal.setId(updatedMedicationRequest.getIdElement());
			retVal.setResource(updatedMedicationRequest);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	@Read()
	public IBaseResource readMedicationRequest(@IdParam IdType theId) {
		IBaseResource retVal = null;

		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	@Search()
	public IBundleProvider findMedicationRequetsById(
			@RequiredParam(name = MedicationRequest.SP_RES_ID) TokenOrListParam theMedicationRequestId,

			@IncludeParam(allow = { "MedicationRequest:medication" }) final Set<Include> theIncludes

	) {

		if (theMedicationRequestId == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theMedicationRequestId.getValuesAsQueryTokens()) {
			whereStatement += "mr.id = " + theCondition.getValue() + " OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM medicationrequest mr " + whereStatement;
		String query = "SELECT * FROM medicationrequest mr " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findMedicationRequestsByParams(
			@OptionalParam(name = MedicationRequest.SP_CODE) TokenOrListParam theOrCodes,
			@OptionalParam(name = MedicationRequest.SP_MEDICATION + "."
					+ Medication.SP_CODE) TokenOrListParam theMedicationOrCodes,
			@OptionalParam(name = MedicationRequest.SP_MEDICATION, chainWhitelist = {
					"" }) ReferenceParam theMedication,
			@OptionalParam(name = MedicationRequest.SP_ENCOUNTER) ReferenceParam theEncounter,
			@OptionalParam(name = MedicationRequest.SP_AUTHOREDON) DateParam theDate,
			@OptionalParam(name = MedicationRequest.SP_PATIENT, chainWhitelist = { "", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceOrListParam thePatients,
			@OptionalParam(name = MedicationRequest.SP_SUBJECT, chainWhitelist = { "", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceOrListParam theSubjects,
			@Sort SortSpec theSort,

			@IncludeParam(allow = { "MedicationRequest:medication" }) final Set<Include> theIncludes

	) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = "medicationrequest mr";
		if (theOrCodes != null) {
			fromStatement = constructFromStatement(theOrCodes, fromStatement, "codes", "mr");
			String where = constructCodeWhereParameter(theOrCodes);
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theEncounter != null) {
			whereParameters.add("mr.resource->encounter->>reference like '%" + theEncounter.getIdPart() + "%'");
		}

		if (theDate != null) {
			String where = constructDateWhereParameter(theDate, "mr", "authoredOn");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theMedicationOrCodes != null) {
			fromStatement = constructFromStatement(theOrCodes, fromStatement, "codes",
					"mr.resource->'medicationCodeableConcept'");
			String where = constructCodeWhereParameter(theOrCodes);
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theMedication != null) {
			whereParameters
					.add("mr.resource->medicationReference->>reference like '%" + theMedication.getIdPart() + "%'");
		}

		if (theSubjects != null) {
			for (ReferenceParam theSubject : theSubjects.getValuesAsQueryTokens()) {
				String where = constructSubjectWhereParameter(theSubject, "mr");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
		}

		if (thePatients != null) {
			for (ReferenceParam thePatient : thePatients.getValuesAsQueryTokens()) {
				String where = constructPatientWhereParameter(thePatient, "mr");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

//	private void mapParameter(Map<String, List<ParameterWrapper>> paramMap, String FHIRparam, Object value, boolean or) {
//		List<ParameterWrapper> paramList = myMapper.mapParameter(FHIRparam, value, or);
//		if (paramList != null) {
//			paramMap.put(FHIRparam, paramList);
//		}
//	}

	private void errorProcessing(String msg) {
		OperationOutcome outcome = new OperationOutcome();
		CodeableConcept detailCode = new CodeableConcept();
		detailCode.setText(msg);
		outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
		throw new UnprocessableEntityException(FhirContext.forDstu3(), outcome);
	}

	private void validateResource(MedicationRequest theMedication) {
		// TODO: implement validation method
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
		Set<Include> theIncludes;

		public MyBundleProvider(String query, Set<Include> theIncludes) {
			super(query);
			setPreferredPageSize(preferredPageSize);
			this.theIncludes = theIncludes;
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();

			// _Include
			List<String> includes = new ArrayList<String>();

			if (theIncludes.contains(new Include("MedicationRequest:medication"))) {
				includes.add("MedicationRequest:medication");
			}

			if (toIndex - fromIndex > 0) {
				query += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			try {
				retVal.addAll(getFhirbaseMapping().search(query, getResourceType()));
			} catch (SQLException e) {
				e.printStackTrace();
			}

			return retVal;
		}
	}
}
