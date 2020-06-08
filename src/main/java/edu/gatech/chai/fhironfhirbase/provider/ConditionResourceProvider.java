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

import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.operation.FhirbaseMapping;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.stereotype.Service;

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
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;

/**
 * This is a simple resource provider which only implements "read/GET" methods,
 * but which uses a custom subclassed resource definition to add statically
 * bound extensions.
 *
 * See the MyOrganization definition to see how the custom resource definition
 * works.
 */
@Service
public class ConditionResourceProvider extends BaseResourceProvider {
	protected FhirbaseMapping fhirbaseMapping;
	private int preferredPageSize = 30;

	public ConditionResourceProvider() {
		super();
	}

	@PostConstruct
    private void postConstruct() {
		getFhirbaseMapping().setFhirClass(Condition.class);
		getFhirbaseMapping().setTableName(ConditionResourceProvider.getType().toLowerCase());
		setMyResourceType(ConditionResourceProvider.getType());
		
		getTotalSize("SELECT count(*) FROM "+getFhirbaseMapping().getTableName()+";");
	}

	public static String getType() {
		return "Condition";
	}

	/**
	 * The getResourceType method comes from IResourceProvider, and must be
	 * overridden to indicate what type of resource this provider supplies.
	 */
	@Override
	public Class<Condition> getResourceType() {
		return Condition.class;
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createCondition(@ResourceParam Condition condition) {
		return create(condition);
	}

	@Delete()
	public void deleteCondition(@IdParam IdType theId) {
		delete(theId);
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
	public IBaseResource readCondition(@IdParam IdType theId) {
		return read(theId, getResourceType(), "condition");
	}

	/**
	 * The "@Update" annotation indicates that this method supports replacing an
	 * existing resource (by ID) with a new instance of that resource.
	 *
	 * @param theId        This is the ID of the patient to update
	 * @param theCondition This is the actual resource to save
	 * @return This method returns a "MethodOutcome"
	 */
	@Update()
	public MethodOutcome updateCondition(@IdParam IdType theId, @ResourceParam Condition theCondition) {
		validateResource(theCondition);

		return update(theId, theCondition, getResourceType());
	}

	@Search()
	public IBundleProvider findConditionByIds(
			@RequiredParam(name = Condition.SP_RES_ID) TokenOrListParam theConditionIds) {
		if (theConditionIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theConditionIds.getValuesAsQueryTokens()) {
			whereStatement += "c.id = " + theCondition.getValue() + " OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM condition c " + whereStatement;
		String query = "SELECT * FROM condition c " + whereStatement;
		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findConditionByParams(@OptionalParam(name = Condition.SP_CODE) TokenOrListParam theOrCodes,
			@OptionalParam(name = Condition.SP_SUBJECT) ReferenceOrListParam theSubjects,
			@OptionalParam(name = Condition.SP_PATIENT, chainWhitelist = { "", USCorePatient.SP_NAME }) ReferenceOrListParam thePatients,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = "condition c";
		if (theOrCodes != null) {
			String where = constructCodeWhereParameter(theOrCodes, fromStatement, "c.resource->'code'");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theSubjects != null) {
			for (ReferenceParam theSubject : theSubjects.getValuesAsQueryTokens()) {
				String where = constructSubjectWhereParameter(theSubject, "c");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
		}

		if (thePatients != null) {
			for (ReferenceParam thePatient : thePatients.getValuesAsQueryTokens()) {
				String where = constructPatientWhereParameter(thePatient, "c");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
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

	// TODO: Add more validation code here.
	private void validateResource(Condition theCondition) {
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
		public MyBundleProvider(String query) {
			super(query);
			setPreferredPageSize(preferredPageSize);
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			// _Include
			// TODO: do this later
			List<String> includes = new ArrayList<String>();

			if (toIndex - fromIndex > 0) {
				query += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			return search(query, getResourceType());
		}

	}
}
