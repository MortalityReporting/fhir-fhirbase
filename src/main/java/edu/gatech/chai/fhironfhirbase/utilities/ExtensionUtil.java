package edu.gatech.chai.fhironfhirbase.utilities;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Resource;

import ca.uhn.fhir.parser.IParser;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;

public class ExtensionUtil {

	// Extension related fixed values
	public static String extTrackingNumberUrl = "http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number";
	public static String extTrackingNumberTypeSystem = "http://hl7.org/fhir/us/mdi/CodeSystem/cs-mdi-codes";
	public static Coding mdiCaseNumber = new Coding(ExtensionUtil.extTrackingNumberTypeSystem, "mdi-case-number", "MDI Case Number");
	public static Coding edrsFileNumber = new Coding(ExtensionUtil.extTrackingNumberTypeSystem, "edrs-file-number", "EDRS File Number");
	public static Coding toxLabCaseNumber = new Coding(ExtensionUtil.extTrackingNumberTypeSystem, "tox-lab-case-number", "Toxicology Laboratory Case Number");
	public static Coding funeralHomeCaseNumber = new Coding(ExtensionUtil.extTrackingNumberTypeSystem, "funeral-home-case-number", "Funeral Home Case Number");

	public static Map<String, Long>  resourceCounts = new HashMap<String, Long>();
	
	public static USCorePatient usCorePatientFromResource(Resource resource) {
		IParser p = OperationUtil.MyFhirContext.newJsonParser();
		String patientJSON = p.encodeResourceToString(resource);

		return p.parseResource(USCorePatient.class, patientJSON);
	}
	
	public static Map<String, Long> getResourceCounts () {
		return ExtensionUtil.resourceCounts;
	}
	
	public static void setResourceCounts (Map<String, Long> resourceCounts) {
		ExtensionUtil.resourceCounts = resourceCounts;
	}
	
	public static void addResourceCount (String resourceName, Long count) {
		Map<String, Long> counts = ExtensionUtil.getResourceCounts();
		
		counts.put(resourceName, count);
	}
	
	public static Long getResourceCount (String resourceName) {
		Map<String, Long> counts = ExtensionUtil.getResourceCounts();
		Long count = counts.get(resourceName);
		
		if (count == null) return 0L;
		
		return count;
	}
}
