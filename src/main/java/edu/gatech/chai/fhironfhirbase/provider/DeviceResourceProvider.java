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
import org.hl7.fhir.r4.model.IdType;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
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
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.model.MyDevice;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;

@Service
@Scope("prototype")
public class DeviceResourceProvider extends BaseResourceProvider {

	private int preferredPageSize = 30;

	public DeviceResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
    private void postConstruct() {
		setTableName(DeviceResourceProvider.getType().toLowerCase());
		setMyResourceType(DeviceResourceProvider.getType());
		getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
	}

	@Override
	public Class<MyDevice> getResourceType() {
		return MyDevice.class;
	}

	public static String getType() {
		return "Device";
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 * 
	 * OMOP DeviceExposure is more like device usage info. So, the device resource
	 * does not have enough information to create DeviceExposure in OMOP. Device
	 * needs to be created from DeviceUseStatement resource.
	 */
//	@Create()
//	public MethodOutcome createDevice(@ResourceParam Device theDevice) {
//		validateResource(theDevice);
//		
//		Long id=null;
//		try {
//			id = getMyMapper().toDbase(theDevice, null);
//		} catch (FHIRException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}		
//		return new MethodOutcome(new IdDt(id));
//	}

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
	public MethodOutcome updateDevice(@IdParam IdType theId, @ResourceParam MyDevice theDevice) {
		validateResource(theDevice);
		MethodOutcome retVal = new MethodOutcome();
		try {
			IBaseResource updatedDevice = getFhirbaseMapping().update(theDevice, getResourceType());
			retVal.setId(updatedDevice.getIdElement());
			retVal.setResource(updatedDevice);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Delete()
	public void deleteDevice(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Search()
	public IBundleProvider findDevicesById(@RequiredParam(name = MyDevice.SP_RES_ID) TokenOrListParam theDeviceId) {

		if (theDeviceId == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theDeviceId.getValuesAsQueryTokens()) {
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
	public IBundleProvider findDevicesByParams(
			@OptionalParam(name=MyDevice.SP_PATIENT, chainWhitelist={"", USCorePatient.SP_NAME}) ReferenceParam thePatient, 
			@OptionalParam(name=MyDevice.SP_TYPE) TokenOrListParam theOrTypes,
			@Sort SortSpec theSort
			) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = "device d";

		if (theOrTypes != null) {
			fromStatement = constructFromStatementPath(fromStatement, "types", "d.resource->'type'->'coding'");
			String where = constructTypeWhereParameter(theOrTypes);
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}
		
		if (thePatient != null) {
			String where = constructPatientWhereParameter(thePatient, "d");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);		

		if (whereStatement == null || whereStatement.isEmpty()) return null;

		String queryCount = "SELECT count(*) FROM "+fromStatement+whereStatement;
		String query = "SELECT * FROM "+fromStatement+whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		
		return myBundleProvider;
	}

	// TODO: Add more validation code here.
	private void validateResource(MyDevice theDevice) {
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
		public MyBundleProvider(String query) {
			super(query);
			setPreferredPageSize(preferredPageSize);
		}

		@Override
		public List<IBaseResource> getResources(int theFromIndex, int theToIndex) {
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();
			// _Include
			// TODO: do this later
			List<String> includes = new ArrayList<String>();

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
