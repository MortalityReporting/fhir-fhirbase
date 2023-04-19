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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.Endpoint;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryResponseComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Composition.CompositionAttesterComponent;
import org.hl7.fhir.r4.model.Composition.CompositionEventComponent;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Practitioner;
import org.hl7.fhir.r4.model.PractitionerRole;
import org.hl7.fhir.r4.model.Procedure;
import org.hl7.fhir.r4.model.Procedure.ProcedurePerformerComponent;
import org.hl7.fhir.r4.model.Specimen.SpecimenContainerComponent;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.http.HttpStatus;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Practitioner.PractitionerQualificationComponent;
import org.hl7.fhir.r4.model.Location;
import org.hl7.fhir.r4.model.MessageHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.Specimen;
import org.hl7.fhir.r4.model.Type;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import ca.uhn.fhir.rest.gclient.TokenClientParam;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.model.MyBundle;
import edu.gatech.chai.fhironfhirbase.utilities.OperationUtil;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;

public class SystemTransactionProvider {
	private static final Logger logger = LoggerFactory.getLogger(SystemTransactionProvider.class);

	private FhirContext ctx;
	private Map<String, String> referenceIds;

	public SystemTransactionProvider() {
	}

	public SystemTransactionProvider(FhirContext ctx) {
		this.ctx = ctx;
	}

	public static String getType() {
		return "Bundle";
	}

	public Class<? extends Resource> getResourceClass(String resourceName) {
		if ("Patient".equals(resourceName)) {
			return Patient.class;
		} else if ("Observation".equals(resourceName)) {
			return Observation.class;
		} else if ("Condition".equals(resourceName)) {
			return Condition.class;
		} else if ("Procedure".equals(resourceName)) {
			return Procedure.class;
		} else {
			return null;
		}
	}

	private void processDelete(IGenericClient client, List<BundleEntryComponent> entries) {
		for (BundleEntryComponent entry : entries) {
			BundleEntryResponseComponent response = entry.getResponse();
			if (response != null && !response.isEmpty()) {
				// We have already processed this.
				continue;
			}

			BundleEntryRequestComponent request = entry.getRequest();
			HTTPVerb requestMethod = request.getMethod();

			if (requestMethod == null) {
				continue;
			}
			
			// We only handles DELETE from the entries.
			if (requestMethod.equals(HTTPVerb.DELETE)) {
				response = entry.getResponse();
				if (request.hasUrl()) {
					String[] url = request.getUrl().split("/");
					if (url.length != 2) {
						response.setStatus(String.valueOf(HttpStatus.BAD_REQUEST.value())
								+ HttpStatus.BAD_REQUEST.getReasonPhrase());
					} else {
						MethodOutcome fhirResponse = (MethodOutcome) client.delete().resourceById(url[0], url[1])
								.execute();
						OperationOutcome outcome = (OperationOutcome) fhirResponse.getOperationOutcome();
						if (outcome != null) {
							response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()) + " "
									+ outcome.getIssueFirstRep().getDetails().getCodingFirstRep().getCode());
						} else {
							response.setStatus(String.valueOf(HttpStatus.OK.value()));
						}
					}
				} else {
					response.setStatus(String.valueOf(HttpStatus.BAD_REQUEST.value()));
				}

				entry.setRequest(null);
			}
		}
	}

	private Patient processPostPatient(IGenericClient client, BundleEntryComponent entry) {
		Patient patient = null;

		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Patient)) {
			return patient;
		}

		// patient = (Patient) resource;
		// Patient retPatient = (Patient) checkForExisting(client, entry, patient.getIdentifier(), Patient.IDENTIFIER, PatientResourceProvider.getType(), response);
		// if (retPatient != null) {
		// 	return retPatient;
		// }

		patient = (Patient) createResource(client, entry, PatientResourceProvider.getType(), response);

		return patient;
	}

	private Endpoint processPostEndpoint(IGenericClient client, BundleEntryComponent entry) {
		Endpoint endpoint = null;

		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Endpoint)) {
			return null;
		}

		endpoint = (Endpoint) createResource(client, entry, EndpointResourceProvider.getType(), response);

		updateReference(endpoint.getManagingOrganization());

		return endpoint;
	}

	private Organization processPostOrganization(IGenericClient client, BundleEntryComponent entry) {
		Organization organization = null;

		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Organization)) {
			return null;
		}

		organization = (Organization) createResource(client, entry, OrganizationResourceProvider.getType(), response);

		updateReference(organization.getPartOf());
		for (Reference endpoint : organization.getEndpoint()) {
			updateReference(endpoint);
		}

		return organization;
	}

	private Practitioner processPostPractitioner(IGenericClient client, BundleEntryComponent entry) {
		Practitioner practitioner = null;

		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Practitioner)) {
			return null;
		}

		practitioner = (Practitioner) createResource(client, entry, PractitionerResourceProvider.getType(), response);

		for (PractitionerQualificationComponent qualification : practitioner.getQualification()) {
			updateReference(qualification.getIssuer());
		}

		return practitioner;
	}

	private PractitionerRole processPostPractitionerRole(IGenericClient client, BundleEntryComponent entry) {
		PractitionerRole practitionerRole = null;

		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof PractitionerRole)) {
			return null;
		}

		practitionerRole = (PractitionerRole) createResource(client, entry, PractitionerRoleResourceProvider.getType(), response);

		updateReference(practitionerRole.getPractitioner());
		updateReference(practitionerRole.getOrganization());
		for (Reference location : practitionerRole.getLocation()) {
			updateReference(location);
		}
		for (Reference healthCareService : practitionerRole.getHealthcareService()) {
			updateReference(healthCareService);
		}
		for (Reference endpoint : practitionerRole.getEndpoint()) {
			updateReference(endpoint);
		}

		return practitionerRole;
	}

	private Location processPostLocation(IGenericClient client, BundleEntryComponent entry) {
		Location location = null;
		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Location)) {
			return  null;
		}

		location = (Location) createResource(client, entry, LocationResourceProvider.getType(), response);

		return location;
	}
	
	private Condition processPostCondition(IGenericClient client, BundleEntryComponent entry) {
		Condition condition = null;
		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Condition)) {
			return null;
		}

		condition = (Condition) createResource(client, entry, ConditionResourceProvider.getType(), response);
		updateReference(condition.getSubject());
		updateReference(condition.getAsserter());	
		
		return condition;
	}

	private Specimen processPostSpecimen(IGenericClient client, BundleEntryComponent entry) {
		Specimen specimen = null;
		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Specimen)) {
			return null;
		}

		// specimen = (Specimen) resource;
		// Specimen retSpecimen= (Specimen) checkForExisting(client, entry, specimen.getIdentifier(), Specimen.IDENTIFIER, SpecimenResourceProvider.getType(), response);
		// if (retSpecimen != null) {
		// 	return retSpecimen;
		// }

		specimen = (Specimen) createResource(client, entry, SpecimenResourceProvider.getType(), response);
		
		updateReference(specimen.getSubject());

		// any parent specimen reference update
		for (Reference reference : specimen.getParent()) {
			updateReference(reference);
		}

		// Container additive reference update (if any)
		for (SpecimenContainerComponent container : specimen.getContainer()) {
			Type containerAdditive = container.getAdditive();
			if (containerAdditive != null && containerAdditive instanceof Reference) {
				Reference reference = (Reference) containerAdditive;
				updateReference(reference);
			}
		}

		return specimen;
	}


	private Observation processPostObservation(IGenericClient client, BundleEntryComponent entry) {
		Observation observation = null;
		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Observation)) {
			return null;
		}

		// observation = (Observation) resource;
		// Observation retObservation = (Observation) checkForExisting(client, entry, observation.getIdentifier(), Observation.IDENTIFIER, ObservationResourceProvider.getType(), response);
		// if (retObservation != null) {
		// 	return retObservation;
		// }

		observation = (Observation) createResource(client, entry, ObservationResourceProvider.getType(), response);

		updateReference(observation.getSubject());
		for (Reference reference : observation.getPerformer()) {
			updateReference(reference);	
		}
		
		return observation;
	}

	private DiagnosticReport processPostDiagnositicReport(IGenericClient client, BundleEntryComponent entry) {
		DiagnosticReport diagnosticReport = null;
		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof DiagnosticReport)) {
			return null;
		}

		// diagnosticReport = (DiagnosticReport) resource;
		// DiagnosticReport retObservation = (DiagnosticReport) checkForExisting(client, entry, diagnosticReport.getIdentifier(), DiagnosticReport.IDENTIFIER,  DiagnosticReportResourceProvider.getType(), response);
		// if (retObservation != null) {
		// 	return retObservation;
		// }

		diagnosticReport = (DiagnosticReport) createResource(client, entry, DiagnosticReportResourceProvider.getType(), response);
		
		updateReference(diagnosticReport.getSubject());
		for (Reference reference : diagnosticReport.getPerformer()) {
			updateReference(reference);	
		}
		// Specimen needs 
		for (Reference reference : diagnosticReport.getSpecimen()) {
			updateReference(reference);	
		}
		for (Reference reference : diagnosticReport.getResult()) {
			updateReference(reference);	
		}

		return diagnosticReport;
	}

	private Procedure processPostProcedure(IGenericClient client, BundleEntryComponent entry) {
		Procedure procedure = null;
		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Procedure)) {
			return null;
		}

		// procedure = (Procedure) resource;
		// Procedure retProcedure = (Procedure) checkForExisting(client, entry, procedure.getIdentifier(), Procedure.IDENTIFIER, ProcedureResourceProvider.getType(), response);
		// if (retProcedure != null) {
		// 	return retProcedure;
		// }

		procedure = (Procedure) createResource(client, entry, ProcedureResourceProvider.getType(), response);
		
		updateReference(procedure.getSubject());
		ProcedurePerformerComponent performer = procedure.getPerformerFirstRep();
		if (performer != null && !performer.isEmpty()) {
			updateReference(performer.getActor());
		}
		updateReference(procedure.getAsserter());

		return procedure;
	}

	private Resource checkForExisting(IGenericClient client, BundleEntryComponent entry, List<Identifier> identifiers, TokenClientParam tokenClientParam, String resourceType, BundleEntryResponseComponent response) {
		Resource resource = entry.getResource();
		for (Identifier identifier : identifiers) {
			Bundle responseBundle = client
					.search().forResource(resourceType).where(tokenClientParam.exactly()
							.systemAndCode(identifier.getSystem(), identifier.getValue()))
					.returnBundle(Bundle.class).execute();

			int total = responseBundle.getTotal();
			if (total > 0) {
				Resource existingResource = responseBundle.getEntryFirstRep().getResource();
				IdType resourceId = existingResource.getIdElement();

				logger.debug("Existing " + resourceType + " > Identifier - System: " + identifier.getSystem() + ", Value:" + identifier.getValue());

				addReference(entry.getFullUrl(), resourceType, resource.getIdElement().getIdPart(), resourceId.getIdPart());
				response.setStatus(HttpStatus.CONFLICT.name() + "- Practitioner Exists");
				response.setLocation(existingResource.getIdElement().asStringValue());
				entry.setRequest(null);
				entry.setFullUrl(resourceType + "/" + resourceId.getIdPart());

				return existingResource;
			}
		}

		return null;
	}

	private Resource createResource(IGenericClient client, BundleEntryComponent entry, String resourceType, BundleEntryResponseComponent response) {
		Resource resource = entry.getResource();
		MethodOutcome outcome;
		boolean created = false;
		if (resource.getIdElement() != null && !resource.isEmpty()) {
			outcome = client.update().resource(resource).prettyPrint().encodedJson().execute();
		} else {
			outcome = client.create().resource(resource).prettyPrint().encodedJson().execute();
			created = true;
		}

		String resourceId = outcome.getId().getIdPart();
		OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
		if (oo == null) {
			referenceIds.put(resourceType + "/" + resource.getIdElement().getIdPart(), resourceType +"/" + resourceId);

			if (entry.getFullUrl() != null && !entry.getFullUrl().isEmpty()) {
				addReference(entry.getFullUrl(), resourceType, resource.getIdElement().getIdPart(), resourceId);
			}

			if (created) {
				response.setStatus(
						String.valueOf(HttpStatus.CREATED.value()) + " " + HttpStatus.CREATED.getReasonPhrase());
			} else {
				response.setStatus(
						String.valueOf(HttpStatus.OK.value()) + " " + HttpStatus.OK.getReasonPhrase());				
			}

			response.setLocation(resourceType + "/" + resourceId);

			entry.setFullUrl(resourceType + "/" + resourceId);
			if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
				entry.setResource((Resource) outcome.getResource());
			}
		} else {
			throw new UnprocessableEntityException("Unable to create/update " + resourceType + " : " + resourceId, oo);
		}

		entry.setRequest(null);

		return resource;
	}

	private void addReference(String fullUrl, String resourceName, String oldId, String newId) {

		if (fullUrl != null && !fullUrl.isEmpty()) {
			referenceIds.put(fullUrl, resourceName + "/" + newId);
		}

		referenceIds.put(resourceName + "/" + oldId, resourceName + "/" + newId);
	}

	private void updateReference(Reference reference) {
		if (reference == null || reference.isEmpty())
			return;

		String originalId = reference.getReferenceElement().getValueAsString();
		String newId = referenceIds.get(originalId);

		logger.debug("original id: " + originalId + " new id:" + newId);
		if (newId != null && !newId.isEmpty()) {
			String[] resourceId = newId.split("/");
			if (resourceId.length == 2) {
				reference.setReferenceElement(new IdType(resourceId[0], resourceId[1]));
			} else {
				reference.setReferenceElement(new IdType(newId));
			}
		}
	}

	Composition searchComposition (IGenericClient client, TokenClientParam tokenClientParam, String system, String value) {
		Bundle resultBundle = client.search().forResource(Composition.class).where(tokenClientParam.exactly().systemAndCode(system, value)).returnBundle(Bundle.class).execute();
		if (resultBundle != null && !resultBundle.isEmpty()) {
			BundleEntryComponent compositionEntry = resultBundle.getEntryFirstRep();
			if (compositionEntry != null && !compositionEntry.isEmpty()) {
				return (Composition) compositionEntry.getResource();
			}
		}
		
		return null;
	}

	Composition searchComposition (IGenericClient client, Composition composition) {
		// If patientId does not exist, we use tracking number
		List<Extension> trackingNumExts = composition.getExtensionsByUrl("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number");
		for (Extension trackingNumExt : trackingNumExts) {
			Identifier trackingNumIdentifier = (Identifier) trackingNumExt.getValue();
			String code = trackingNumIdentifier.getType().getCodingFirstRep().getCode();
			String system = StringUtils.defaultString(trackingNumIdentifier.getSystem());
			String value = StringUtils.defaultString(trackingNumIdentifier.getValue());
			if ("mdi-case-number".equals(code)) {	
				return searchComposition(client, CompositionResourceProvider.MDI_CASE_NUMBER, system, value);
			} else if ("edrs-file-number".equals(code)) {
				return searchComposition(client, CompositionResourceProvider.EDRS_FILE_NUMBER, system, value);
			} else if ("tox-lab-case-number".equals(code)) {
				return searchComposition(client, DiagnosticReportResourceProvider.TOX_LAB_CASE_NUMBER, system, value);
			}
		}

		return null;
	}

	DiagnosticReport searchDiagnosticReport (IGenericClient client, TokenClientParam tokenClientParam, String system, String value) {
		Bundle resultBundle = client.search().forResource(DiagnosticReport.class).where(tokenClientParam.exactly().systemAndCode(system, value)).returnBundle(Bundle.class).execute();
		if (resultBundle != null && !resultBundle.isEmpty()) {
			BundleEntryComponent diagnosticReportEntry = resultBundle.getEntryFirstRep();
			if (diagnosticReportEntry != null && !diagnosticReportEntry.isEmpty()) {
				return (DiagnosticReport) diagnosticReportEntry.getResource();
			}
		}
		
		return null;
	}

	DiagnosticReport searchDiagnosticReport (IGenericClient client, DiagnosticReport diagnosticReport) {
		// If patientId does not exist, we use tracking number
		List<Extension> trackingNumExts = diagnosticReport.getExtensionsByUrl("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number");
		for (Extension trackingNumExt : trackingNumExts) {
			Identifier trackingNumIdentifier = (Identifier) trackingNumExt.getValue();
			String code = trackingNumIdentifier.getType().getCodingFirstRep().getCode();
			String system = StringUtils.defaultString(trackingNumIdentifier.getSystem());
			String value = StringUtils.defaultString(trackingNumIdentifier.getValue());
			if ("mdi-case-number".equals(code)) {	
				return searchDiagnosticReport(client, DiagnosticReportResourceProvider.MDI_CASE_NUMBER, system, value);
			} else if ("tox-lab-case-number".equals(code)) {
				return searchDiagnosticReport(client, DiagnosticReportResourceProvider.TOX_LAB_CASE_NUMBER, system, value);
			}
		}

		return null;
	}

	boolean compareTrackingValue(String code, String system, String value, List<Extension> trackingNumbExts) {
		for (Extension trackingNumbExt : trackingNumbExts) {
			Identifier trackingNumIdentifier = (Identifier) trackingNumbExt.getValue();
			String code_ = trackingNumIdentifier.getType().getCodingFirstRep().getCode();
			String system_ = StringUtils.defaultString(trackingNumIdentifier.getSystem());
			String value_ = StringUtils.defaultString(trackingNumIdentifier.getValue());
			
			if (code.equals(code_)) {
				if (system.equals(system_) && value.equals(value_)) {
					return true;
				}
			}
		}

		return false;
	}

	int compareCompositionsTrackingNumber(Composition c1, Composition c2) {
		int resultCode = 0;
		List<Extension> trackingNumExtsC1 = c1.getExtensionsByUrl("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number");
		List<Extension> trackingNumExtsC2 = c2.getExtensionsByUrl("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number");
		for (Extension trackingNumExtC1 : trackingNumExtsC1) {
			Identifier trackingNumIdentifierC1 = (Identifier) trackingNumExtC1.getValue();
			String code1 = trackingNumIdentifierC1.getType().getCodingFirstRep().getCode();
			String system1 = StringUtils.defaultString(trackingNumIdentifierC1.getSystem());
			String value1 = StringUtils.defaultString(trackingNumIdentifierC1.getValue());
			if ("mdi-case-number".equals(code1)) {
				if (compareTrackingValue("mdi-case-number", system1, value1, trackingNumExtsC2)) {
					resultCode += 1;
				}
			} else if ("edrs-file-number".equals(code1)) {
				if (compareTrackingValue("edrs-file-number", system1, value1, trackingNumExtsC2)) {
					resultCode += 10;
				}
			}
		}

		return resultCode;
	}

	int compareDiagnosticReportsTrackingNumber(DiagnosticReport d1, DiagnosticReport d2) {
		int resultCode = 0;
		List<Extension> trackingNumExtsC1 = d1.getExtensionsByUrl("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number");
		List<Extension> trackingNumExtsC2 = d2.getExtensionsByUrl("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number");
		for (Extension trackingNumExtC1 : trackingNumExtsC1) {
			Identifier trackingNumIdentifierC1 = (Identifier) trackingNumExtC1.getValue();
			String code1 = trackingNumIdentifierC1.getType().getCodingFirstRep().getCode();
			String system1 = StringUtils.defaultString(trackingNumIdentifierC1.getSystem());
			String value1 = StringUtils.defaultString(trackingNumIdentifierC1.getValue());
			if ("mdi-case-number".equals(code1)) {
				if (compareTrackingValue("mdi-case-number", system1, value1, trackingNumExtsC2)) {
					resultCode += 1;
				}
			} else if ("tox-lab-case-number".equals(code1)) {
				if (compareTrackingValue("tox-lab-case-number", system1, value1, trackingNumExtsC2)) {
					resultCode += 10;
				}
			}
		}

		return resultCode;
	}

	private MessageHeader processPostMessageHeader(IGenericClient client, BundleEntryComponent entry) {
		MessageHeader messageHeader = null;
		BundleEntryResponseComponent response = entry.getResponse();

		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof MessageHeader)) {
			return null;
		}

		System.out.println("++++++++++++++++++++++++");
		
		// messageHeader = (MessageHeader) resource;
		messageHeader = (MessageHeader) createResource(client, entry, MessageHeaderResourceProvider.getType(), response);

		// MessageHeaders will not be checked for the duplication (not identifier exists)		
		for (Reference focuseReference : messageHeader.getFocus()) {
			updateReference(focuseReference);
		}
		
		entry.setRequest(null);
	
		return messageHeader;
	}

	private Composition processPostComposition(IGenericClient client, BundleEntryComponent entry) {
		Composition composition = null;
		BundleEntryResponseComponent response = entry.getResponse();
		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Composition)) {
			return null;
		}

		System.out.println("++++++++++++++++++++++++");
		
		// composition = (Composition) resource;

		// Search if this composition exists
		// Composition existComposition = searchComposition(client, composition);
		// if (existComposition != null) {
		// 	// Composition exists
		// 	IdType resourceId = existComposition.getIdElement();
		// 	addReference(entry.getFullUrl(), CompositionResourceProvider.getType(), resource.getIdElement().getIdPart(), resourceId.getIdPart());
		// 	response.setStatus(HttpStatus.CONFLICT.name() + "- Composition Exists");
		// 	response.setLocation(existComposition.getIdElement().asStringValue());
		// 	entry.setRequest(null);
		// 	entry.setFullUrl(CompositionResourceProvider.getType() + "/" + resourceId.getIdPart());

		// 	return existComposition;
		// }

		composition = (Composition) createResource(client, entry, CompositionResourceProvider.getType(), response);

		updateReference(composition.getSubject());
		for (Reference author: composition.getAuthor()) {
			updateReference(author);
		}

		CompositionAttesterComponent attester = composition.getAttesterFirstRep();
		if (attester != null && !attester.isEmpty()) {
			updateReference(attester.getParty());						
		}
		
		CompositionEventComponent event = composition.getEventFirstRep();
		if (event != null && !event.isEmpty()) {
			updateReference(event.getDetailFirstRep());
		}
		
		List<SectionComponent> sectionComponents = composition.getSection();
		for (SectionComponent sectionComponent : sectionComponents) {
			updateReference(sectionComponent.getFocus());

			List<Reference> references = sectionComponent.getEntry();
			for (Reference reference : references) {
				updateReference(reference);
			}

			references = sectionComponent.getAuthor();
			for (Reference reference : references) {
				updateReference(reference);
			}
		}
		
		entry.setRequest(null);
	
		return composition;
	}

	private Bundle processPostBundle(IGenericClient client, BundleEntryComponent entry) {
		Bundle bundle = null;
		BundleEntryResponseComponent response = entry.getResponse();

		if (!response.isEmpty()) {
			// We have already processed this.
			return null;
		}

		Resource resource = entry.getResource();
		if (!(resource instanceof Bundle)) {
			return null;
		}
		
		bundle = (Bundle) createResource(client, entry, BundleResourceProvider.getType(), response);

		// Bundle has an entry. If it's not empty, process that
		processEntries(client, bundle.getEntry());
		
		entry.setRequest(null);
	
		return bundle;
	}

	private void checkIfDocumentOkToProceed(IGenericClient client, Composition composition) {
		Composition searchedComposition = searchComposition(client, composition);
		if (searchedComposition == null || searchedComposition.isEmpty()) {
			// this is a new one. No need to compare.
			return;
		}

		int resultCode = compareCompositionsTrackingNumber(composition, searchedComposition);
		if (resultCode > 0) {
			String error_message = "Case exists with";
			if (!searchedComposition.getIdPart().equals(composition.getIdPart())) {
				// IDs are not same. This is collision. This will create
				// duplicate data. So, we should reject this and do not proceed.
				if (resultCode % 10 == 1) {
					error_message = error_message.concat(" mdi-case-number");
				}

				if (resultCode / 10 == 1) {
					error_message = error_message.concat(" edrs-file-number");
				}

				ThrowFHIRExceptions.unprocessableEntityException(error_message);
			}
		}
	}

	private void checkIfDiagnosticReportOkToProceed(IGenericClient client, DiagnosticReport diagnosticReport) {
		DiagnosticReport searchedDiagnosticReport = searchDiagnosticReport(client, diagnosticReport);
		if (searchedDiagnosticReport == null || searchedDiagnosticReport.isEmpty()) {
			// this is a new one. No need to compare.
			return;
		}

		int resultCode = compareDiagnosticReportsTrackingNumber(diagnosticReport, searchedDiagnosticReport);
		if (resultCode > 0) {
			String error_message = "Case exists with";
			if (!searchedDiagnosticReport.getIdPart().equals(diagnosticReport.getIdPart())) {
				// IDs are not same. This is collision. This will create
				// duplicate data. So, we should reject this and do not proceed.
				if (resultCode % 10 == 1) {
					error_message = error_message.concat(" mdi-case-number");
				}

				if (resultCode / 10 == 1) {
					error_message = error_message.concat(" tox-lab-case-number");
				}

				ThrowFHIRExceptions.unprocessableEntityException(error_message);
			}
		}
	}

	/***
	 * processPost: process MDI-IG bundle document data
	 * @param client
	 * @param entries
	 */
	private void processEntries(IGenericClient client, List<BundleEntryComponent> entries) {
		List<BundleEntryComponent> postEntries = new ArrayList<BundleEntryComponent>();
		BundleEntryResponseComponent response;

		for (BundleEntryComponent entry : entries) {
			response = entry.getResponse();
			if (response != null && !response.isEmpty()) {
				// We have already processed this.
				continue;
			}
			
			String originalFullUrl = entry.getFullUrl();
			
			int idPartIndex = originalFullUrl.lastIndexOf("/");
			String originalFullUrlIdPart = originalFullUrl.substring(idPartIndex+1);

			BundleEntryRequestComponent request = entry.getRequest();
			HTTPVerb requestMethod;
			if (request == null || request.isEmpty()) {
				requestMethod = HTTPVerb.POST;
			} else {
				requestMethod = request.getMethod();
			}

			// Create POST entries
			if (requestMethod.equals(HTTPVerb.POST)) {
				postEntries.add(entry);
			}
		}

		// Process Patient first from POST entries.
		// String patientId = null;
		// Patient patient = null;
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Patient) {
					processPostPatient(client, entry);
					// patientId = patient.getIdElement().getIdPart();
				}
			}
		}

		// It's important the order of POST processing because of references.
		// Endpoint
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Endpoint) {
					processPostEndpoint(client, entry);
				}
			}
		}

		// Organization
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Organization) {
					processPostOrganization(client, entry);
				}
			}
		}

		// Practitioner
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Practitioner) {
					processPostPractitioner(client, entry);
				}
			}
		}

		// PractitionerRole
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof PractitionerRole) {
					processPostPractitionerRole(client, entry);
				}
			}
		}
		
		// Location
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Location) {
					processPostLocation(client, entry);
				}
			}
		}

		// Condition
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Condition) {
					processPostCondition(client, entry);
				}
			}
		}

		// Specimen - should be after subject and condition (and parent specimen)
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Specimen) {
					processPostSpecimen(client, entry);
				}
			}
		}

		// Observation (includes toxicology lab results)
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Observation) {
					processPostObservation(client, entry);
				}
			}
		}

		// Procedure
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Procedure) {
					processPostProcedure(client, entry);
				}
			}
		}

		// Composition
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Composition) {
					processPostComposition(client, entry);
				}
			}
		}		

		// DiagnosticReport
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof DiagnosticReport) {
					processPostDiagnositicReport(client, entry);
				}
			}
		}		
		
		// MessageHeader
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof MessageHeader) {
					processPostMessageHeader(client, entry);
				}
			}
		}		

		// Bundle
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				if (resource instanceof Bundle) {
					processPostBundle(client, entry);
				}
			}
		}		
		
	}

	/**
	 * 
	 * @param theBundle
	 * @param theRequest
	 * @return
	 */
	@Transaction
	public Bundle transaction(@TransactionParam MyBundle theBundle, HttpServletRequest theRequest) {
		validateResource(theBundle);

		List<BundleEntryComponent> entries = theBundle.getEntry();

		if (entries.size() == 0) {
			return null;
		}

		IGenericClient client = ctx.newRestfulGenericClient(OperationUtil.myHostUrl());

		String authBasic = System.getenv("AUTH_BASIC");
		String authBearer = System.getenv("AUTH_BEARER");
		if (authBasic != null && !authBasic.isEmpty()) {
			String[] auth = authBasic.split(":");
			if (auth.length == 2) {
				client.registerInterceptor(new BasicAuthInterceptor(auth[0], auth[1]));
			}
		} else if (authBearer != null && !authBearer.isEmpty()) {
			client.registerInterceptor(new BearerTokenAuthInterceptor(authBearer));
		}

				// Duplication is not allowed. Check if this will cause the duplication. If it does,
		// we send OO to avoid that.

		// Check the first resource in the entry. We only worry about Composition and MessageHeader
		BundleEntryComponent entryComponent = entries.get(0);
		Resource resource = entryComponent.getResource();
		if (resource == null) {
			return null;
		}

		if (resource instanceof Composition) {
			// This is MDI-to-EDRS document. 
			checkIfDocumentOkToProceed(client, (Composition) resource);
		} else if (resource instanceof MessageHeader) {
			// This is Toxicology-to-CMS document if DiagnosticReport is at the focus.
			MessageHeader messageHeader = (MessageHeader) resource;
			Reference focus = messageHeader.getFocusFirstRep();
			if (focus != null && !focus.isEmpty()) {
				String focusReferenceId = focus.getReferenceElement().getValueAsString();
				Resource focusResource = null;
				for (BundleEntryComponent entry : entries) {
					String entryId = entry.getFullUrl();
					if (focusReferenceId != null && focusReferenceId.equals(entryId)) {
						focusResource = entry.getResource();
						break;
					}
				}

				if (focusResource != null && !focusResource.isEmpty() && focusResource instanceof DiagnosticReport) {
					checkIfDiagnosticReportOkToProceed(client, (DiagnosticReport) focusResource);
				}
			}
		}

		// First save this bundle to fhirbase 
		MethodOutcome outcome;
		if (theBundle.getIdElement() != null) {
			outcome = client.update().resource(theBundle).prettyPrint().encodedJson().execute();
		} else {
			outcome = client.create().resource(theBundle).prettyPrint().encodedJson().execute();
		}

		// The bundle couldn't be created in the fhirbase.
		OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
		if (oo != null && !oo.isEmpty()) {
			throw new InternalErrorException("Bundle Transanction Operation Error", oo);
		}

		referenceIds = new HashMap<String, String>();

		switch (theBundle.getType()) {
		case BATCH:
			// We process for the following order as suggested by FHIR spec
			// https://hl7.org/FHIR/http.html#transaction
			// 1. Delete
			// 2. Post
			// 3. Put or Patch
			// 4. Get or Head
			// 5. Conditional Reference

			processDelete(client, entries);
			processEntries(client, entries);
			theBundle.setType(BundleType.BATCHRESPONSE);

			break;
		case DOCUMENT:
		case MESSAGE:
			// MDI-to-EDRS document (for now)
			processDelete(client, entries);
			processEntries(client, entries);
			break;
		default:
		}

		return theBundle;
	}

	private void validateResource(MyBundle theBundle) {
		// We must have request for each entry
//		OperationOutcome outcome = new OperationOutcome();
//		CodeableConcept detailCode = new CodeableConcept();
//		for (BundleEntryComponent entry : theBundle.getEntry()) {
//			BundleEntryRequestComponent request = entry.getRequest();
//			if (request == null || request.isEmpty()) {
//				detailCode.setText(entry.getFullUrl() + " does not have a request.");
//				outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
//				throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
//			}
//		}
	}

	public Bundle mdiUpdateProcess(MyBundle theBundle) {
		
		List<BundleEntryComponent> entries = theBundle.getEntry();

		if (entries.size() == 0)
			return null;

//		String requestUrl = theRequest.getRequestURL().toString();
		// It's better to use the requestUrl. But, redirection seems to be causing hiccups.
		String requestUrl = System.getenv("INTERNAL_FHIR_REQUEST_URL");
		if (requestUrl == null || requestUrl.isEmpty()) {
			requestUrl = "http://localhost:8080/fhir";
		}
		IGenericClient client = ctx.newRestfulGenericClient(requestUrl);

		String authBasic = System.getenv("AUTH_BASIC");
		String authBearer = System.getenv("AUTH_BEARER");
		if (authBasic != null && !authBasic.isEmpty()) {
			String[] auth = authBasic.split(":");
			if (auth.length == 2) {
				client.registerInterceptor(new BasicAuthInterceptor(auth[0], auth[1]));
			}
		} else if (authBearer != null && !authBearer.isEmpty()) {
			client.registerInterceptor(new BearerTokenAuthInterceptor(authBearer));
		}

		// First save this bundle to fhirbase if POST/PUT
//		client.create().resource(theBundle).prettyPrint().encodedJson().execute();

		referenceIds = new HashMap<String, String>();

		switch (theBundle.getType()) {
		case BATCH:
			// We process for the following order as suggested by FHIR spec
			// https://hl7.org/FHIR/http.html#transaction
			// 1. Delete
			// 2. Post
			// 3. Put or Patch
			// 4. Get or Head
			// 5. Conditional Reference

			processDelete(client, entries);
			processEntries(client, entries);
			
			theBundle.setType(BundleType.BATCHRESPONSE);

			break;
		default:
			throw new FHIRException("Only BATCH transaction is supported");
		}

		return theBundle;
	}

}
