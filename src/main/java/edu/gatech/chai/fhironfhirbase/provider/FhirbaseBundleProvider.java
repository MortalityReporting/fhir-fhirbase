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

import java.util.Date;

import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.model.InstantType;

import ca.uhn.fhir.rest.api.server.IBundleProvider;

public abstract class FhirbaseBundleProvider implements IBundleProvider {
	InstantType searchTime;
	String query;
	Integer preferredPageSize;
	Integer totalSize;

	protected FhirbaseBundleProvider (String query) {
		this.searchTime = InstantType.withCurrentTime();
		this.query = query;
	}
	
	public void setPreferredPageSize(Integer preferredPageSize) {
		this.preferredPageSize = preferredPageSize;
	}
	
	public void setTotalSize(Integer totalSize) {
		this.totalSize = totalSize;
	}

	@Override
	public IPrimitiveType<Date> getPublished() {
		return searchTime;
	}

	@Override
	public String getUuid() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer preferredPageSize() {
		return this.preferredPageSize;
	}

	@Override
	public Integer size() {
		return this.totalSize;
	}

}
