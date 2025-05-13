package edu.gatech.chai.fhironfhirbase.utilities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Procedure;
import org.hl7.fhir.r4.model.RelatedPerson;

public class MdiProfileUtil {

	private MdiProfileUtil() {}

	public static Coding MDI_EDRS_DC = new Coding("http://loinc.org", "86807-5", "Death administrative information Document"); 
	public static Coding TOXICOLOGY_LAB_RESULT = new Coding("http://hl7.org/fhir/us/mdi/CodeSystem/cs-mdi-codes", "tox-result-report", "Toxicology Lab Results"); 
	public static Coding DCR_REPORT = new Coding("http://hl7.org/fhir/us/mdi/CodeSystem/cs-mdi-codes", "death-certificate-data-review-doc", "Death Certificate Data Review Document"); 
	public static Coding DCR_REPORT_EVENT = new Coding("http://hl7.org/fhir/us/mdi/CodeSystem/cs-mdi-codes", "death-certificate-review-event", "Death Certificate Review Event"); 
	
	private static Map<String, String> singletonMap;
	static {
		singletonMap = new HashMap<String, String>();
		singletonMap.put("Observation", 
				"http://loinc.org:30525-0,"         // VRDR-Decedent-Age
				+ "http://loinc.org:80913-7,"       // VRDR-Decedent-Education-Level
				+ "http://hl7.org/fhir/v2/0203:BR," // VRDR-BirthRecordIdentifier
				+ "http://loinc.org:74497-9,"       // VRDR-Examiner-Contacted
				+ "http://loinc.org:69442-2,"       // VRDR-Decedent-Pregnancy
				+ "http://loinc.org:69451-3,"       // VRDR-Decedent-Transportation-Role
				+ "http://loinc.org:69443-0,2.16.840.1.113883.6.1:69443-0," // VRDR-Tobacco-Use-Contributed-To-Death
				+ "http://loinc.org:85699-7,"       // VRDR-Autopsy-Performed-Indicator
				+ "http://loinc.org:11374-6,"       // VRDR-InjuryIncident
				+ "http://loinc.org:81956-5,"       // VRDR-Death-Date
				+ "http://loinc.org:69449-7,"       // VRDR-Manner-of-Death
				+ "http://loinc.org:80905-3,"       // VRDR-Decedent-Disposition-Method
				+ "http://loinc.org:21843-8"        // VRDR-Decedent-Usual-Work
				); 
		singletonMap.put("RelatedPerson", 
				"http://terminology.hl7.org/CodeSystem/v3-RoleCode:MTH,"   // VRDR-Decedent-Mother
				+ "http://terminology.hl7.org/CodeSystem/v3-RoleCode:SPS," // VRDR-Decedent-Spouse
				+ "http://terminology.hl7.org/CodeSystem/v3-RoleCode:FTH"  // VRDR-Decedent-Father
				);
		singletonMap.put("Composition", 
				"http://loinc.org:64297-5"         // VRDR-Death-Certificate
				);
		singletonMap.put("Procedure", 
				"http://snomed.info/sct:308646001" // VRDR-Death-Certification
				);
	}
	
	private static Coding isCodeableConceptContained(String resourceName, CodeableConcept codeableConcept) {
		if (codeableConcept == null || codeableConcept.isEmpty())
			return null;
		
		List<Coding> codings = codeableConcept.getCoding();
		for (Coding coding: codings) {
			String code = coding.getCode();
			
			// it seems code itself is sufficient to check.
			String keyVal = MdiProfileUtil.singletonMap.get(resourceName);
			if (keyVal.contains(code)) {
				return coding;
			}
		}
		
		return null;
	}
	
	public static Coding isSingletonResource(IBaseResource resource) {
		// For Observations
		if (resource instanceof Observation) {
			Observation obs = (Observation) resource;
			return MdiProfileUtil.isCodeableConceptContained("Observation", obs.getCode());
		} else if (resource instanceof RelatedPerson) {
			RelatedPerson relatedPerson = (RelatedPerson) resource;
			List<CodeableConcept> relationships = relatedPerson.getRelationship();
			for (CodeableConcept relationship: relationships) {
				Coding myCoding = MdiProfileUtil.isCodeableConceptContained("RelatedPerson", relationship);
				if (myCoding != null && !myCoding.isEmpty()) {
					return myCoding;
				}
			}
		} else if (resource instanceof Composition) {
			Composition composition = (Composition) resource;
			return MdiProfileUtil.isCodeableConceptContained("Composition", composition.getType());
		} else if (resource instanceof Procedure) {
			Procedure procedure = (Procedure) resource;
			return MdiProfileUtil.isCodeableConceptContained("Procedure", procedure.getCode());
		}
		
		return null;
	}
}
