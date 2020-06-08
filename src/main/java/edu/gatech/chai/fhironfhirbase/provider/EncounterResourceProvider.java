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
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.stereotype.Service;

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
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;

@Service
public class EncounterResourceProvider extends BaseResourceProvider {

	private int preferredPageSize = 30;

	public EncounterResourceProvider() {
		super();
	}

	@PostConstruct
    private void postConstruct() {
		getFhirbaseMapping().setFhirClass(Encounter.class);
		getFhirbaseMapping().setTableName(EncounterResourceProvider.getType().toLowerCase());
		setMyResourceType(EncounterResourceProvider.getType());
		
		getTotalSize("SELECT count(*) FROM "+getFhirbaseMapping().getTableName()+";");
	}

	@Override
	public Class<Encounter> getResourceType() {
		return Encounter.class;
	}

	public static String getType() {
		return "Encounter";
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createEncounter(@ResourceParam Encounter theEncounter) {
		validateResource(theEncounter);

		return create(theEncounter);
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
	public IBaseResource readEncounter(@IdParam IdType theId) {

		return read(theId, getResourceType(), "encounter");
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
	public MethodOutcome updateEncounter(@IdParam IdType theId, @ResourceParam Encounter theEncounter) {
		validateResource(theEncounter);

		return update(theId, theEncounter, getResourceType());
	}

	@Delete()
	public void deleteEncounter(@IdParam IdType theId) {
		delete(theId);
	}

	@Search()
	public IBundleProvider findEncounterByParams(@OptionalParam(name = Encounter.SP_RES_ID) TokenParam theEncounterId,
			@OptionalParam(name = Encounter.SP_PATIENT, chainWhitelist = { "",
					USCorePatient.SP_NAME }) ReferenceOrListParam thePatients,
			@OptionalParam(name = Encounter.SP_SUBJECT) ReferenceOrListParam theSubjects,
			@OptionalParam(name = Encounter.SP_DIAGNOSIS) ReferenceParam theDiagnosis, @Sort SortSpec theSort,

			@IncludeParam(allow = { "Encounter:appointment", "Encounter:diagnosis", "Encounter:episodeofcare",
					"Encounter:incomingreferral", "Encounter:location", "Encounter:part-of", "Encounter:participant",
					"Encounter:service-provider", "Encounter:patient", "Encounter:practitioner",
					"Encounter:subject" }) final Set<Include> theIncludes,

			@IncludeParam(reverse = true) final Set<Include> theReverseIncludes) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = "encounter e";

		if (theEncounterId != null) {
			whereParameters.add("e.id = " + theEncounterId.getValue());
		}

		// With OMOP, we only support subject to be patient.
		// If the subject has only ID part, we assume that is patient.
		if (theSubjects != null) {
			for (ReferenceParam theSubject : theSubjects.getValuesAsQueryTokens()) {
				String where = constructSubjectWhereParameter(theSubject, "e");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
		}

		if (thePatients != null) {
			for (ReferenceParam thePatient : thePatients.getValuesAsQueryTokens()) {
				String where = constructPatientWhereParameter(thePatient, "e");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
		}

		if (theDiagnosis != null) {
			whereParameters.add("e.resource->'diagnosis'->>'condition' like '%" + theDiagnosis.getValue() + "%'");
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, theReverseIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	// TODO: Add more validation code here.
	private void validateResource(Encounter theEncounter) {
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
			// TODO: include later.
			List<String> includes = new ArrayList<String>();

			if (theIncludes.contains(new Include("Encounter:appointment"))) {
				includes.add("Encounter:appointment");
			}

			if (theIncludes.contains(new Include("Encounter:diagnosis"))) {
				includes.add("Encounter:diagnosis");
			}

			if (theIncludes.contains(new Include("Encounter:incomingreferral"))) {
				includes.add("Encounter:incomingreferral");
			}

			if (theIncludes.contains(new Include("Encounter:location"))) {
				includes.add("Encounter:location");
			}

			if (theIncludes.contains(new Include("Encounter:part-of"))) {
				includes.add("Encounter:part-of");
			}

			if (theIncludes.contains(new Include("Encounter:participant"))) {
				includes.add("Encounter:participant");
			}

			if (theIncludes.contains(new Include("Encounter:service-provider"))) {
				includes.add("Encounter:service-provider");
			}

			if (theIncludes.contains(new Include("Encounter:patient"))) {
				includes.add("Encounter:patient");
			}

			if (theIncludes.contains(new Include("Encounter:practitioner"))) {
				includes.add("Encounter:practitioner");
			}

			if (theIncludes.contains(new Include("Encounter:subject"))) {
				includes.add("Encounter:subject");
			}

			if (toIndex - fromIndex > 0) {
				query += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			return search(query, getResourceType());
		}
	}
}
