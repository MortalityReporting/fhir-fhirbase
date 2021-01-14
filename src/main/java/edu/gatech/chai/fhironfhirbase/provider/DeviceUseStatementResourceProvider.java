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
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
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
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.model.MyDeviceUseStatement;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class DeviceUseStatementResourceProvider extends BaseResourceProvider {

	private int preferredPageSize = 30;

	public DeviceUseStatementResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(DeviceUseStatementResourceProvider.getType().toLowerCase());
		setMyResourceType(DeviceUseStatementResourceProvider.getType());
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	@Override
	public Class<MyDeviceUseStatement> getResourceType() {
		return MyDeviceUseStatement.class;
	}

	public static String getType() {
		return "DeviceUseStatement";
	}

	/***
	 * 
	 * @param theDeviceUseStatement
	 * @return
	 * 
	 *         This creates a DeviceExposure entry. Since the table servers Devce
	 *         and DeviceUseStatement, Create request should embed Device FHIR
	 *         information in the DeviceUseStatement resource.
	 */
	@Create()
	public MethodOutcome createDeviceUseStatement(@ResourceParam MyDeviceUseStatement theDeviceUseStatement) {
		validateResource(theDeviceUseStatement);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdDeviceUseStatement = getFhirbaseMapping().create(theDeviceUseStatement, getResourceType());
			retVal.setId(createdDeviceUseStatement.getIdElement());
			retVal.setResource(createdDeviceUseStatement);
			retVal.setCreated(true);
		} catch (SQLException e) {
			retVal.setCreated(false);
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

		return retVal;
	}

	@Read()
	public IBaseResource readPatient(@IdParam IdType theId) {
		IBaseResource retVal = null;
		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Update()
	public MethodOutcome updateDeviceUseStatement(@IdParam IdType theId,
			@ResourceParam MyDeviceUseStatement theDeviceUseStatement) {
		validateResource(theDeviceUseStatement);

		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedDeviceUseStatement = getFhirbaseMapping().update(theDeviceUseStatement, getResourceType());
			retVal.setId(updatedDeviceUseStatement.getIdElement());
			retVal.setResource(updatedDeviceUseStatement);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Delete()
	public void deleteDeviceUseStatement(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	@Search()
	public IBundleProvider findDeviceUseStatementsByParams(
			@OptionalParam(name = MyDeviceUseStatement.SP_RES_ID) TokenOrListParam theDeviceUseStatementIds,
			@OptionalParam(name = MyDeviceUseStatement.SP_PATIENT) ReferenceOrListParam thePatients,
			@OptionalParam(name = MyDeviceUseStatement.SP_SUBJECT, chainWhitelist = { "",
					USCorePatient.SP_NAME }) ReferenceOrListParam theSubjects,
			@Sort SortSpec theSort,
			@IncludeParam(allow = { "DeviceUseStatement:device" }) final Set<Include> theIncludes

	) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = "deviceusestatement du";

		if (theDeviceUseStatementIds != null) {
			for (TokenParam theDeviceUseStatementId : theDeviceUseStatementIds.getValuesAsQueryTokens()) {
				whereParameters.add("du.id = '" + theDeviceUseStatementId.getValue() + "'");
			}
			returnAll = false;
		}

		// With OMOP, we only support subject to be patient.
		// If the subject has only ID part, we assume that is patient.
		if (theSubjects != null) {
			for (ReferenceParam theSubject : theSubjects.getValuesAsQueryTokens()) {
				String where = constructSubjectWhereParameter(theSubject, "du");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
			returnAll = false;
		}

		if (thePatients != null) {
			for (ReferenceParam thePatient : thePatients.getValuesAsQueryTokens()) {
				String where = constructPatientWhereParameter(thePatient, "du");
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

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	// TODO: Add more validation code here.
	private void validateResource(MyDeviceUseStatement theDeviceUseStatement) {
	}

	private void errorProcessing(String msg) {
		OperationOutcome outcome = new OperationOutcome();
		CodeableConcept detailCode = new CodeableConcept();
		detailCode.setText(msg);
		outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
		throw new UnprocessableEntityException(FhirContext.forDstu3(), outcome);
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
		Set<Include> theIncludes;

		public MyBundleProvider(String query, Set<Include> theIncludes) {
			super(query);
			setPreferredPageSize(preferredPageSize);
			this.theIncludes = theIncludes;
		}

		@Override
		public List<IBaseResource> getResources(int theFromIndex, int theToIndex) {
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();
			
			// _Include
			// TODO: do the include later
			List<String> includes = new ArrayList<String>();
			if (theIncludes.contains(new Include("DeviceUseStatement:device"))) {
				includes.add("DeviceUseStatement:device");
			}

			String myQuery = query;			
			if (theToIndex - theFromIndex > 0) {
				myQuery += " LIMIT " + (theToIndex - theFromIndex) + " OFFSET " + theFromIndex;
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
