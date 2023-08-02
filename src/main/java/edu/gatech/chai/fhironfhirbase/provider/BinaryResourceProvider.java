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
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class BinaryResourceProvider extends BaseResourceProvider {

	public BinaryResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(BinaryResourceProvider.getType().toLowerCase());
		setMyResourceType(BinaryResourceProvider.getType().replace("\"", ""));

		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	public static String getType() {
		return "\"Binary\"";
	}

	@Override
	public Class<Binary> getResourceType() {
		return Binary.class;
	}

	@Create()
	public MethodOutcome createBinary(@ResourceParam Binary theBinary) {
		validateResource(theBinary);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdBinary = getFhirbaseMapping().create(theBinary, getResourceType());
			retVal.setId(createdBinary.getIdElement());
			retVal.setResource(createdBinary);
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
	public void deleteBinary(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	@Read()
	public IBaseResource readBinary(@IdParam IdType theId) {
		IBaseResource retVal = null;
		
		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Update()
	public MethodOutcome updateBinary(@IdParam IdType theId,
			@ResourceParam Binary theBinary) {
		validateResource(theBinary);

		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedBinary = getFhirbaseMapping().update(theBinary, getResourceType());
			retVal.setId(updatedBinary.getIdElement());
			retVal.setResource(updatedBinary);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Search()
	public IBundleProvider findDocumentReferenceByIds(
			@OptionalParam(name = Binary.SP_RES_ID) TokenOrListParam theBinaryIds) {

		String whereStatement = "";
		if (theBinaryIds != null) {
			whereStatement = " WHERE ";
			for (TokenParam theBinaryId : theBinaryIds.getValuesAsQueryTokens()) {
				whereStatement += "b.id = '" + theBinaryId.getValue() + "' OR ";
			}

			whereStatement = whereStatement.substring(0, whereStatement.length() - 4);
		}

		String queryCount = "SELECT count(*) FROM \"binary\" b" + whereStatement;
		String query = "SELECT * FROM \"binary\" b" + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, null, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
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

	/* TODO: 
	 * Add more validation code here.
	 */
	private void validateResource(Binary theBinary) {
		
	}

}
