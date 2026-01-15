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

import edu.gatech.chai.MDI.model.resource.util.CommonUtil;

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

	public static Coding usCoreRaceConceptFromCode(String code) {
		// set up US core race coding.

		if ("1002-5".equals(code)) {
			return new Coding("urn:oid:2.16.840.1.113883.6.238", code, "American Indian or Alaska Native");
		}
		
		if ("2028-9".equals(code)) {
			return new Coding("urn:oid:2.16.840.1.113883.6.238", code, "Asian");
		}

		if ("2054-5".equals(code)) {
			return new Coding("urn:oid:2.16.840.1.113883.6.238", code, "Black or African American");
		}

		if ("2076-8".equals(code)) {
			return new Coding("urn:oid:2.16.840.1.113883.6.238", code, "Native Hawaiian or Other Pacific Islander");
		}

		if ("2106-3".equals(code)) {
			return new Coding("urn:oid:2.16.840.1.113883.6.238", code, "White");
		}

		if ("2131-1".equals(code)) {
			return new Coding("urn:oid:2.16.840.1.113883.6.238", code, "Other Race");
		}

		if ("ASKU".equalsIgnoreCase(code)) {
			return CommonUtil.askedButUnknownCode.getCodingFirstRep();
		}

		if ("UNK".equalsIgnoreCase(code)) {
			return CommonUtil.unknownCode.getCodingFirstRep();
		}

		return null;
	}

	public static Coding usCoreEthnicityConceptFromCode(String code) {
		// set up US core race coding.

		if ("2135-2".equals(code)) {
			return new Coding("urn:oid:2.16.840.1.113883.6.238", code, "Hispanic or Latino");
		}
		
		if ("2186-5".equals(code)) {
			return new Coding("urn:oid:2.16.840.1.113883.6.238", code, "Not Hispanic or Latino");
		}

		if ("ASKU".equalsIgnoreCase(code)) {
			return CommonUtil.askedButUnknownCode.getCodingFirstRep();
		}

		if ("UNK".equalsIgnoreCase(code)) {
			return CommonUtil.unknownCode.getCodingFirstRep();
		}

		return null;
	}
}
