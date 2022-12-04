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
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.DocumentReference;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryResponseComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Composition.CompositionAttesterComponent;
import org.hl7.fhir.r4.model.Composition.CompositionEventComponent;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Practitioner;
import org.hl7.fhir.r4.model.Procedure;
import org.hl7.fhir.r4.model.Procedure.ProcedurePerformerComponent;
import org.hl7.fhir.r4.model.Specimen.SpecimenContainerComponent;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RelatedPerson;
import org.springframework.http.HttpStatus;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.ListResource;
import org.hl7.fhir.r4.model.ListResource.ListEntryComponent;
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
import edu.gatech.chai.fhironfhirbase.model.MyBundle;
import edu.gatech.chai.fhironfhirbase.utilities.OperationUtil;

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

		patient = (Patient) resource;
		Patient retPatient = (Patient) checkForExisting(client, entry, patient.getIdentifier(), Patient.IDENTIFIER, PatientResourceProvider.getType(), response);
		if (retPatient != null) {
			return retPatient;
		}

		patient = (Patient) createResource(client, entry, PatientResourceProvider.getType(), response);

		return patient;
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
		practitioner = (Practitioner) resource;
		Practitioner retPractitioner = (Practitioner) checkForExisting(client, entry, practitioner.getIdentifier(), Practitioner.IDENTIFIER, PractitionerResourceProvider.getType(), response);
		if (retPractitioner != null) {
			return retPractitioner;
		}

		practitioner = (Practitioner) createResource(client, entry, PractitionerResourceProvider.getType(), response);

		return practitioner;
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

		location = (Location) resource;
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

		condition = (Condition) resource;
		Condition retCondition = (Condition) checkForExisting(client, entry, condition.getIdentifier(), Condition.IDENTIFIER, ConditionResourceProvider.getType(), response);
		if (retCondition != null) {
			return retCondition;
		}

		updateReference(condition.getSubject());
		updateReference(condition.getAsserter());	
		condition = (Condition) createResource(client, entry, ConditionResourceProvider.getType(), response);
		
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

		specimen = (Specimen) resource;
		Specimen retSpecimen= (Specimen) checkForExisting(client, entry, specimen.getIdentifier(), Specimen.IDENTIFIER, SpecimenResourceProvider.getType(), response);
		if (retSpecimen != null) {
			return retSpecimen;
		}

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

		specimen = (Specimen) createResource(client, entry, SpecimenResourceProvider.getType(), response);
		
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

		observation = (Observation) resource;
		Observation retObservation = (Observation) checkForExisting(client, entry, observation.getIdentifier(), Observation.IDENTIFIER, ObservationResourceProvider.getType(), response);
		if (retObservation != null) {
			return retObservation;
		}

		updateReference(observation.getSubject());
		for (Reference reference : observation.getPerformer()) {
			updateReference(reference);	
		}

		observation = (Observation) createResource(client, entry, ObservationResourceProvider.getType(), response);
		
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

		diagnosticReport = (DiagnosticReport) resource;
		DiagnosticReport retObservation = (DiagnosticReport) checkForExisting(client, entry, diagnosticReport.getIdentifier(), DiagnosticReport.IDENTIFIER,  DiagnosticReportResourceProvider.getType(), response);
		if (retObservation != null) {
			return retObservation;
		}

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

		diagnosticReport = (DiagnosticReport) createResource(client, entry, DiagnosticReportResourceProvider.getType(), response);
		
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

		procedure = (Procedure) resource;
		Procedure retProcedure = (Procedure) checkForExisting(client, entry, procedure.getIdentifier(), Procedure.IDENTIFIER, ProcedureResourceProvider.getType(), response);
		if (retProcedure != null) {
			return retProcedure;
		}

		updateReference(procedure.getSubject());
		ProcedurePerformerComponent performer = procedure.getPerformerFirstRep();
		if (performer != null && !performer.isEmpty()) {
			updateReference(performer.getActor());
		}
		updateReference(procedure.getAsserter());

		procedure = (Procedure) createResource(client, entry, ProcedureResourceProvider.getType(), response);
		
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
		MethodOutcome outcome = client.create().resource(resource).prettyPrint().encodedJson().execute();
		if (outcome.getCreated()) {
			IIdType resourceId = outcome.getId();
			if (entry.getFullUrl() != null && !entry.getFullUrl().isEmpty()) {
				addReference(entry.getFullUrl(), resourceType, resource.getIdElement().getIdPart(), resourceId.getIdPart());
			}

			response.setStatus(
					String.valueOf(HttpStatus.CREATED.value()) + " " + HttpStatus.CREATED.getReasonPhrase());
			response.setLocation(resourceType + "/" + resourceId);

			entry.setFullUrl(resourceType + "/" + resourceId);
			if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
				entry.setResource((Resource) outcome.getResource());
			}

			resource.setId(resourceId);
		} else {
			response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()) + " "
					+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());

			OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
			if (oo != null && !oo.isEmpty()) {
				response.setOutcome(oo);
			} 

			resource = null;
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
		
		messageHeader = (MessageHeader) resource;

		// MessageHeaders will not be checked for the duplication (not identifier exists)		
		for (Reference focuseReference : messageHeader.getFocus()) {
			updateReference(focuseReference);
		}
		
		messageHeader = (MessageHeader) createResource(client, entry, MessageHeaderResourceProvider.getType(), response);

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
		
		composition = (Composition) resource;

		// Search if this composition exists
		Composition existComposition = searchComposition(client, composition);
		if (existComposition != null) {
			// Composition exists
			IdType resourceId = existComposition.getIdElement();
			addReference(entry.getFullUrl(), CompositionResourceProvider.getType(), resource.getIdElement().getIdPart(), resourceId.getIdPart());
			response.setStatus(HttpStatus.CONFLICT.name() + "- Composition Exists");
			response.setLocation(existComposition.getIdElement().asStringValue());
			entry.setRequest(null);
			entry.setFullUrl(CompositionResourceProvider.getType() + "/" + resourceId.getIdPart());

			return existComposition;
		}

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
		
		composition = (Composition) createResource(client, entry, CompositionResourceProvider.getType(), response);

		entry.setRequest(null);
	
		return composition;
	}

	private boolean checkIfDocumentExist(IGenericClient client, List<BundleEntryComponent> entries) {
		boolean retVal = false;

		for (BundleEntryComponent entry : entries) {
			BundleEntryResponseComponent response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource instanceof Composition) {
				Composition composition = (Composition) resource;


			}
		}

		return retVal;
	}

	/***
	 * processPost: process MDI-IG bundle document data
	 * @param client
	 * @param entries
	 */
	private void processBatch(IGenericClient client, List<BundleEntryComponent> entries) {
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
		String patientId = null;
		Patient patient = null;
		for (BundleEntryComponent entry : postEntries) {
			response = entry.getResponse();
			Resource resource = entry.getResource();
			if (resource != null && !resource.isEmpty()) {
				String resourceId = null;
				if (resource instanceof Patient) {
					patient = processPostPatient(client, entry);
					patientId = patient.getIdElement().getIdPart();
				}
			}
		}

		// It's important the order of POST processing because of references.

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

		if (entries.size() == 0)
			return null;

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

		// First save this bundle to fhirbase
		MethodOutcome outcome = client.create().resource(theBundle).prettyPrint().encodedJson().execute();
		if (!outcome.getCreated()) {
			// The bundle couldn't be created in the fhirbase.
			OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
			if (oo != null && !oo.isEmpty()) {
				throw new InternalErrorException("Bundle Transanction Error", oo);
			} else {
				throw new InternalErrorException("Bundle Transanction Error");
			}
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
			processBatch(client, entries);
			theBundle.setType(BundleType.BATCHRESPONSE);

			break;
		case DOCUMENT:
			// We support two kinds of document here. 
			// One is VRDR, the other is Toxicology document
			// We post all in the entries (based on VRDR resources) to server.
			processDelete(client, entries);
			processBatch(client, entries);

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
			processBatch(client, entries);
			
			theBundle.setType(BundleType.BATCHRESPONSE);

			break;
		default:
			throw new FHIRException("Only BATCH transaction is supported");
		}

		return theBundle;
	}

}
