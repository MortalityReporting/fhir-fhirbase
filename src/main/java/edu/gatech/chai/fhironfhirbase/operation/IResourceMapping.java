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
package edu.gatech.chai.fhironfhirbase.operation;

import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Resource;

public interface IResourceMapping {
	public String create(IBaseResource fhirResource) throws Exception;
	public IBaseResource read(IdType id, Class<? extends Resource> fhirClass, String tableName) throws Exception;
	public IBaseResource update (IBaseResource fhirResource, Class<? extends Resource> fhirClass)  throws Exception;
	public IBaseResource delete (IdType id) throws Exception;
	
	public List<IBaseResource> search(String sql, Class<? extends Resource> fhirClass) throws Exception;
	public int getSize(String sql) throws Exception;
	public IBaseResource delete (String tableName, String id, Class<? extends Resource> fhirClass) throws Exception;

}
