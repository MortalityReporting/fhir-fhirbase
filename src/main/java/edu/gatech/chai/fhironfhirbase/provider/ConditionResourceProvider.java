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

import edu.gatech.chai.fhironfhirbase.model.USCorePatient;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.IdType;
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
@Scope("prototype")
public class ConditionResourceProvider extends BaseResourceProvider {
	private int preferredPageSize = 30;

	public ConditionResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setMyResourceType(ConditionResourceProvider.getType());
		setTableName(ConditionResourceProvider.getType().toLowerCase());
		getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
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
		MethodOutcome retVal = new MethodOutcome();
		try {
			Condition createdCondition = (Condition) getFhirbaseMapping().create(condition, getResourceType());
			if (createdCondition != null && !createdCondition.isEmpty()) {
				retVal.setId(createdCondition.getIdElement());
				retVal.setCreated(true);
				retVal.setResource(createdCondition);
			} else {
				retVal.setCreated(false);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			retVal.setCreated(false);
		}
		
		return retVal;
	}

	@Delete()
	public void deleteCondition(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	 * @param theId        This is the ID of the patient to update
	 * @param theCondition This is the actual resource to save
	 * @return This method returns a "MethodOutcome"
	 */
	@Update()
	public MethodOutcome updateCondition(@IdParam IdType theId, @ResourceParam Condition theCondition) {
		validateResource(theCondition);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedCondition = getFhirbaseMapping().update(theCondition, getResourceType());
			retVal.setId(updatedCondition.getIdElement());
			retVal.setResource(updatedCondition);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
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
			@OptionalParam(name = Condition.SP_PATIENT, chainWhitelist = { "",
					USCorePatient.SP_NAME }) ReferenceOrListParam thePatients,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = "condition c";
		if (theOrCodes != null) {
			fromStatement = constructFromStatementPath(fromStatement, "codes", "c.resource->'code'->'coding'");
			String where = constructCodeWhereParameter(theOrCodes);
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
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();

			// _Include
			// TODO: do this later
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
