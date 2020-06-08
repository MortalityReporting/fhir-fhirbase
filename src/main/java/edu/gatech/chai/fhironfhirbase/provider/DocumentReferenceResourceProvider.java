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
import org.hl7.fhir.r4.model.DocumentReference;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.stereotype.Service;

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
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;

@Service
public class DocumentReferenceResourceProvider extends BaseResourceProvider {

	private int preferredPageSize = 30;

	public DocumentReferenceResourceProvider() {
		super();
	}
	
	@PostConstruct
    private void postConstruct() {
		getFhirbaseMapping().setFhirClass(DocumentReference.class);
		getFhirbaseMapping().setTableName(DocumentReferenceResourceProvider.getType().toLowerCase());
		setMyResourceType(DocumentReferenceResourceProvider.getType());
		
		getTotalSize("SELECT count(*) FROM "+getFhirbaseMapping().getTableName()+";");
	}

	public static String getType() {
		return "DocumentReference";
	}

	@Override
	public Class<DocumentReference> getResourceType() {
		return DocumentReference.class;
	}
	
	@Create()
	public MethodOutcome createDocumentReference(@ResourceParam DocumentReference theDocumentReference) {
		validateResource(theDocumentReference);

		return create(theDocumentReference);
	}
	
	@Delete()
	public void deleteDocumentReference(@IdParam IdType theId) {
		delete(theId);
	}
	
	@Read()
	public IBaseResource readDocumentReference(@IdParam IdType theId) {
		return read(theId, getResourceType(), "documentreference");
	}

	@Update()
	public MethodOutcome updateDocumentReference(@IdParam IdType theId, @ResourceParam DocumentReference theDocumentReference) {
		validateResource(theDocumentReference);

		return update(theId, theDocumentReference, getResourceType());
	}

	@Search()
	public IBundleProvider findDocumentReferenceByIds(
			@RequiredParam(name=DocumentReference.SP_RES_ID) TokenOrListParam theDocumentReferenceIds,
			
			@IncludeParam(allow={"DocumentReference:patient", "DocumentReference:subject", 
			"DocumentReference:encounter"})
			final Set<Include> theIncludes,
			
			@IncludeParam(reverse=true)
            final Set<Include> theReverseIncludes
			) {
		

		if (theDocumentReferenceIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theDocumentReferenceIds.getValuesAsQueryTokens()) {
			whereStatement += "r.id = " + theCondition.getValue() + " OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);
		
		String queryCount = "SELECT count(*) FROM documentreference r " + whereStatement;
		String query = "SELECT * FROM documentreference r " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, theReverseIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}
	
	@Search()
	public IBundleProvider findDocumentReferenceByParams(
			@OptionalParam(name=DocumentReference.SP_PATIENT, chainWhitelist = { "", USCorePatient.SP_NAME }) ReferenceOrListParam thePatients,
			@OptionalParam(name=DocumentReference.SP_SUBJECT, chainWhitelist = { "", USCorePatient.SP_NAME }) ReferenceOrListParam theSubjects,
			@OptionalParam(name=DocumentReference.SP_ENCOUNTER) ReferenceParam theEncounter,
			@OptionalParam(name=DocumentReference.SP_TYPE) TokenOrListParam theOrTypes,
			@OptionalParam(name=DocumentReference.SP_DATE) DateParam theDate,
			@Sort SortSpec theSort,
			@IncludeParam(allow={"DocumentReference:patient", "DocumentReference:subject", 
					"DocumentReference:encounter"})
			final Set<Include> theIncludes,
			
			@IncludeParam(reverse=true)
            final Set<Include> theReverseIncludes
			) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = "documentreference dr";
		if (theOrTypes != null) {
			String where = constructTypeWhereParameter(theOrTypes, fromStatement, "dr.resource->'type'");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theDate != null) {
			String where = constructDateWhereParameter(theDate, fromStatement, "dr", "date");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theSubjects != null) {
			for (ReferenceParam theSubject : theSubjects.getValuesAsQueryTokens()) {
				String where = constructSubjectWhereParameter(theSubject, "dr");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
		}

		if (thePatients != null) {
			for (ReferenceParam thePatient : thePatients.getValuesAsQueryTokens()) {
				String where = constructPatientWhereParameter(thePatient, "dr");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
		}

		if (theEncounter != null) {
			whereParameters.add("dr.resource->'context'->>'encounter' like '%" + theEncounter.getValue() + "%'");
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);		
		
		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, theReverseIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;		
	}
	
	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
		Set<Include> theIncludes;
		Set<Include> theReverseIncludes;

		public MyBundleProvider(String query, Set<Include> theIncludes,
				Set<Include> theReverseIncludes) {
			super(query);
			setPreferredPageSize(preferredPageSize);
			this.theIncludes = theIncludes;
			this.theReverseIncludes = theReverseIncludes;
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			// _Include
			List<String> includes = new ArrayList<String>();

			if (theIncludes.contains(new Include("DocumentReference:encounter"))) {
				includes.add("DocumentReference:encounter");
			}

			if (theIncludes.contains(new Include("DocumentReference:patient"))) {
				includes.add("DocumentReference:patient");
			}

			if (theIncludes.contains(new Include("DocumentReference:subject"))) {
				includes.add("DocumentReference:subject");
			}

			if (toIndex - fromIndex > 0) {
				query += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			return search(query, getResourceType());
		}
	}
	
	// TODO: Add more validation code here.
	private void validateResource(DocumentReference theDocumentReference) {
	}

}
