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
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.QuestionnaireResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import ca.uhn.fhir.rest.param.ReferenceAndListParam;
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class QuestionnaireResponseResourceProvider extends BaseResourceProvider {
	private static final Logger logger = LoggerFactory.getLogger(QuestionnaireResponseResourceProvider.class);

	public QuestionnaireResponseResourceProvider(FhirContext ctx) {
		super(ctx);

		setTableName(QuestionnaireResponseResourceProvider.getType().toLowerCase());
		setMyResourceType(QuestionnaireResponseResourceProvider.getType());
	}

	@PostConstruct
	private void postConstruct() {
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	public static String getType() {
		return "QuestionnaireResponse";
	}

	@Override
	public Class<QuestionnaireResponse> getResourceType() {
		return QuestionnaireResponse.class;
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createQuestionnaireResponse(@ResourceParam QuestionnaireResponse theQuestionnaireResponse) {
		validateResource(theQuestionnaireResponse);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdQuestionnaire = getFhirbaseMapping().create(theQuestionnaireResponse, getResourceType());
			retVal.setId(createdQuestionnaire.getIdElement());
			retVal.setResource(createdQuestionnaire);
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
	public void deleteQuestionnaireResponse (@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	@Search()
	public IBundleProvider findQuestionnaireResponseById(
		@RequiredParam(name = QuestionnaireResponse.SP_RES_ID) TokenOrListParam theQuestionnaireResponseIds, 
		@IncludeParam(allow = { "QuestionnaireResponse:questionnaire", "Observation:patient", "Observation:subject" }) final Set<Include> theIncludes,
		@Sort SortSpec theSort) {

		if (theQuestionnaireResponseIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theQuestionnaireResponse : theQuestionnaireResponseIds.getValuesAsQueryTokens()) {
			whereStatement += "qr.id = '" + theQuestionnaireResponse.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM " + getTableName() + " qr " + whereStatement;
		String query = "SELECT * FROM " + getTableName() + " qr " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findQuestionnaireResponsesByParams(
			@OptionalParam(name = QuestionnaireResponse.SP_QUESTIONNAIRE) ReferenceOrListParam theQuestionnaires,
			@OptionalParam(name = QuestionnaireResponse.SP_PATIENT, chainWhitelist = {"", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceAndListParam thePatients,
			@OptionalParam(name = QuestionnaireResponse.SP_SUBJECT) ReferenceAndListParam theSubjects,
			@IncludeParam(allow = { "QuestionnaireResponse:questionnaire", "QuestionnaireResponse:patient", "QuestionnaireResponse:subject" }) final Set<Include> theIncludes,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = getTableName() + " qr";
		
		if (theQuestionnaires != null) {
			for (ReferenceParam theQuestionnaire : theQuestionnaires.getValuesAsQueryTokens()) {
				String where = constructCanonicalWhereParameter(theQuestionnaire, "qr", "questionnaire");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
			returnAll = false;
		}

		if (thePatients != null) {
			fromStatement += " join patient p on qr.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";
			
			String updatedFromStatement = constructFromWherePatients(fromStatement, whereParameters, thePatients);
			if (updatedFromStatement.isEmpty()) {
				// This means that we have unsupported resource. Since this is to search, we should discard all and
				// return null.
				return null;
			}
			fromStatement = updatedFromStatement;
			
			returnAll = false;
		}

		if (theSubjects != null) {
			for (ReferenceOrListParam theReferences : theSubjects.getValuesAsQueryTokens()) {
				String whereOr = "";
				for (ReferenceParam theReference : theReferences.getValuesAsQueryTokens()) {
					if ("Patient".equals(theReference.getResourceType())) {
						if (!fromStatement.contains("patient p")) {
							fromStatement += " join patient p on qr.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";
						}

						String where = constructPatientWhereParameter(theReference);
						if (whereOr.isEmpty()) {
							whereOr = where;
						} else {
							whereOr += " or " + where;
						}
						if (theReference.getChain() != null && !theReference.getChain().isEmpty()) {
							fromStatement = constructFromStatementPatientChain(fromStatement, theReference.getChain());
						}	
					} else {
						String where = "qr.resource->'subject'->>'reference' = '" + theReference.getValue() + "'"; 
						if (whereOr.isEmpty()) {
							whereOr = where;
						} else {
							whereOr += " or " + where;
						}
					}
				}

				if (whereOr != null && !whereOr.isEmpty()) {
					whereParameters.add(whereOr);
				}
			}			

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
	public IBaseResource readQuestionnaireResponse(@IdParam IdType theId) {
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
	public MethodOutcome updateQuestionnaireResponse(@IdParam IdType theId, @ResourceParam QuestionnaireResponse theQuestionnaireResponse) {
		validateResource(theQuestionnaireResponse);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedQuestionnaireResponse = getFhirbaseMapping().update(theQuestionnaireResponse, getResourceType());
			retVal.setId(updatedQuestionnaireResponse.getIdElement());
			retVal.setResource(updatedQuestionnaireResponse);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	private void validateResource(QuestionnaireResponse theQuestionnaireResponse) {
		OperationOutcome outcome = new OperationOutcome();
		CodeableConcept detailCode = new CodeableConcept();
		if (theQuestionnaireResponse.getStatus() == null) {
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
			
			// _Include
			List<String> includes = new ArrayList<String>();

			if (theIncludes.contains(new Include("QuestionnaireResponse:questionnaire"))) {
				includes.add("QuestionnaireResponse:questionnaire");
			}
			
			if (theIncludes.contains(new Include("QuestionnaireResponse:patient"))) {
				includes.add("QuestionnaireResponse:patient");
			}

			if (theIncludes.contains(new Include("QuestionnaireResponse:subject"))) {
				includes.add("QuestionnaireResponse:subject");
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
