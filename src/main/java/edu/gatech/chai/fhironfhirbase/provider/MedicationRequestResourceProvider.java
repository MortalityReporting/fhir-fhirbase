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

	public MedicationRequestResourceProvider() {
		super();
	}

	@PostConstruct
    private void postConstruct() {
		getFhirbaseMapping().setFhirClass(MedicationRequest.class);
		getFhirbaseMapping().setTableName(MedicationRequestResourceProvider.getType().toLowerCase());
		setMyResourceType(MedicationRequestResourceProvider.getType());
		
		getTotalSize("SELECT count(*) FROM "+getFhirbaseMapping().getTableName()+";");
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

		return create(theMedicationRequest);
	}

	@Delete()
	public void deleteMedicationRequest(@IdParam IdType theId) {
		delete(theId);
	}

	@Update()
	public MethodOutcome updateMedicationRequest(@IdParam IdType theId,
			@ResourceParam MedicationRequest theMedicationRequest) {
		validateResource(theMedicationRequest);

		return update(theId, theMedicationRequest);
	}

	@Read()
	public IBaseResource readMedicationRequest(@IdParam IdType theId) {
		return read(theId, getResourceType());
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
			String where = constructCodeWhereParameter(theOrCodes, fromStatement, "mr");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theEncounter != null) {
			whereParameters.add("mr.resource->encounter->>reference like '%" + theEncounter.getIdPart() + "%'");
		}

		if (theDate != null) {
			String where = constructDateWhereParameter(theDate, fromStatement, "mr", "authoredOn");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theMedicationOrCodes != null) {
			String where = constructCodeWhereParameter(theOrCodes, fromStatement,
					"mr.resource->'medicationCodeableConcept'");
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
			// _Include
			List<String> includes = new ArrayList<String>();

			if (theIncludes.contains(new Include("MedicationRequest:medication"))) {
				includes.add("MedicationRequest:medication");
			}

			if (toIndex - fromIndex > 0) {
				query += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			return search(query, getResourceType());
		}
	}
}
