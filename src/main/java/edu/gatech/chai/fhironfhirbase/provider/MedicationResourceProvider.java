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

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Medication;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Sort;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;

@Service
public class MedicationResourceProvider extends BaseResourceProvider {

	private int preferredPageSize = 30;

	public MedicationResourceProvider() {
		super();
	}
	
	@PostConstruct
    private void postConstruct() {
		getFhirbaseMapping().setFhirClass(Medication.class);
		getFhirbaseMapping().setTableName(MedicationResourceProvider.getType().toLowerCase());
		setMyResourceType(MedicationResourceProvider.getType());
		
		getTotalSize("SELECT count(*) FROM "+getFhirbaseMapping().getTableName()+";");
	}

	@Override
	public Class<Medication> getResourceType() {
		return Medication.class;
	}

	public static String getType() {
		return "Medication";
	}

	@Read()
	public IBaseResource readMedication(@IdParam IdType theId) {
		return read(theId, getResourceType());
	}

	@Search()
	public IBundleProvider findMedicationById(
			@RequiredParam(name = Medication.SP_RES_ID) TokenOrListParam theMedicationIds
			) {
		
		if (theMedicationIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theMedicationIds.getValuesAsQueryTokens()) {
			whereStatement += "mr.id = " + theCondition.getValue() + " OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM medication m " + whereStatement;
		String query = "SELECT * FROM medication m " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		
		return myBundleProvider;
	}
	
	@Search()
	public IBundleProvider findMedicationByParams(
			@OptionalParam(name = Medication.SP_CODE) TokenOrListParam theOrCodes,
			@Sort SortSpec theSort
			) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = "medication m";
		if (theOrCodes != null) {
			String where = constructCodeWhereParameter(theOrCodes, fromStatement, "m.resource->'code'");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

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
	 * @param theMedication
	 *            The medication to validate
	 */
	private void validateResource(Medication theMedication) {
		/*
		 * Our server will have a rule that patients must have a family name or
		 * we will reject them
		 */
		// if (thePatient.getNameFirstRep().getFamily().isEmpty()) {
		// OperationOutcome outcome = new OperationOutcome();
		// CodeableConcept detailCode = new CodeableConcept();
		// detailCode.setText("No family name provided, Patient resources must
		// have at least one family name.");
		// outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
		// throw new UnprocessableEntityException(FhirContext.forDstu3(),
		// outcome);
		// }
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
		public MyBundleProvider(String query) {
			super(query);
			setPreferredPageSize(preferredPageSize);
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			// _Include
			List<String> includes = new ArrayList<String>();

			if (toIndex - fromIndex > 0) {
				query += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			return search(query, getResourceType());
		}

	}

}
