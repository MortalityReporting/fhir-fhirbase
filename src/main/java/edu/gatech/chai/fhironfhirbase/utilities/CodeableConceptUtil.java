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
package edu.gatech.chai.fhironfhirbase.utilities;

import org.hl7.fhir.r4.model.Coding;

public class CodeableConceptUtil {
	/**
	 * 
	 * @param coding1
	 * @param coding2
	 * @return 
	 *   1 if only code matches,
	 *   0 if both system and code match,
	 *   -1 if none matches.
	 */
	public static int compareCodings(Coding coding1, Coding coding2) {
		boolean isSystemMatch = false;
		boolean isCodeMatch = false;
		
		if (coding1.hasSystem() && coding1.hasSystem()) {
			if (coding1.getSystem().equals(coding2.getSystem())) {
				isSystemMatch = true;
			}
		}
		
		if (coding1.hasCode() && coding2.hasCode()) {
			if (coding1.getCode().equals(coding2.getCode())) {
				isCodeMatch = true;
			}
		}
		
		if (isSystemMatch && isCodeMatch) return 0;
		if (isCodeMatch) return 1;
		return -1;
	}

}
