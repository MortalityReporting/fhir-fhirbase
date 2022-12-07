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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.DocumentReference.DocumentReferenceContentComponent;
import org.hl7.fhir.r4.model.MessageHeader.MessageHeaderResponseComponent;
import org.hl7.fhir.r4.model.MessageHeader.ResponseType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.DocumentReference;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.MessageHeader;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Practitioner;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.ServiceRequest;
import org.hl7.fhir.r4.model.Specimen;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UriType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.utilities.OperationUtil;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;

public class ServerOperations {
	private static final Logger logger = LoggerFactory.getLogger(ServerOperations.class);

	private FhirContext ctx;
	private Map<String, String> referenceIds;
	private List<String> patientIds;

	public ServerOperations() {
		this(null);
	}

	public ServerOperations(FhirContext ctx) {
		if (ctx != null) {
			this.ctx = ctx;
		}
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

	private void createComposition(IGenericClient client, List<BundleEntryComponent> entries) {
		// This method must be called for lab report ONLY.
		// For the lab report, we must have only one patientId. 
		// If we have more than one patientId, then it must be for something else and
		// we should not create composition.
		if (patientIds.size() != 1) return;
		
		Composition composition = new Composition();
		composition
				.setType(new CodeableConcept(new Coding("http://loinc.org", "11502-2", "Laboratory report")));

		// Walk through the entries again and add the entries to composition
		SectionComponent sectionComponent = new SectionComponent();
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();
			if (resource instanceof MessageHeader) {
				continue;
			}
			
			if (resource instanceof Patient) {
				composition.setSubject(new Reference("Patient/"+resource.getIdElement().getIdPart()));
			}
			sectionComponent.addEntry(new Reference(resource));
		}

		composition.getSection().add(sectionComponent);
		MethodOutcome mo;
		mo = client.create().resource(composition).encodedJson().prettyPrint().execute();

		if (mo.getId() == null) {
			logger.debug("Failed to create a composition for the batch upload.");
		}
	
	}
	
	private void createMessageHeader(IGenericClient client, MessageHeader messageHeader) {
		// We store all the messaging events.
		if (messageHeader.getIdElement() != null && !messageHeader.getIdElement().isEmpty()) {
			client.update().resource(messageHeader).encodedJson().prettyPrint().execute();
		} else {
			client.create().resource(messageHeader).encodedJson().prettyPrint().execute();
		}
	}

	private OperationOutcome processPostResources(IGenericClient client, List<BundleEntryComponent> entries) {
		// Patient and Binary
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();

			// Do the patient first.
			String patientId = null;
			if (resource instanceof Patient) {
				Patient patient = (Patient) resource;
				MethodOutcome outcome;
				if (patient.getIdElement() != null && !patient.getIdElement().isEmpty()) {
					outcome = client.update().resource(patient).prettyPrint().encodedJson().execute();
				} else {
					outcome = client.create().resource(patient).prettyPrint().encodedJson().execute();
				}
				
				OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
				if (oo == null) {
					patientId = outcome.getId().getIdPart();
					referenceIds.put("Patient/" + patient.getId(), "Patient/" + patientId);
					patientIds.add("Patient/" + patientId);

					if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
						entry.setResource((Resource) outcome.getResource());
					} else {
						patient.setId(patientId);
					}
				} else {
					throw new UnprocessableEntityException("Unable to create/update Patient : " + patient.getId(), oo);
				}
			}

			// We do the Binary here as this does not depend on anything but others can reference this
			// if (resource instanceof Binary) {
			// 	Binary binary = (Binary) resource;

			// 	MethodOutcome outcome = client.create().resource(binary).prettyPrint().encodedJson().execute();
			// 	if (outcome.getCreated().booleanValue()) {
			// 		String binaryId = outcome.getId().getIdPart();
			// 		referenceIds.put("Binary/" + binary.getIdElement().getIdPart(), "Binary/" + binaryId);
			// 	} else {
			// 		return (OperationOutcome) outcome.getOperationOutcome();
			// 	}			
			// }
		}

		// Practitioner
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();

			if (resource instanceof Practitioner) {
				Practitioner practitioner = (Practitioner) resource;
				String practitionerId = null;

				MethodOutcome outcome;
				if (practitioner.getIdElement() != null && !practitioner.getIdElement().isEmpty()) {
					outcome = client.update().resource(practitioner).prettyPrint().encodedJson().execute();
				} else {
					outcome = client.create().resource(practitioner).prettyPrint().encodedJson().execute();
				}

				OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
				if (oo == null) {
					practitionerId = outcome.getId().getIdPart();
					referenceIds.put("Practitioner/" + practitioner.getIdElement().getIdPart(), "Practitioner/" + practitionerId);
				} else {
					throw new UnprocessableEntityException("Unable to create/update Practitioner : " + practitioner.getId(), oo);
				}
			}
		}

		// ServiceRequest - todo
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();

			if (resource instanceof ServiceRequest) {
				ServiceRequest serviceRequest = (ServiceRequest) resource;
				updateReference(serviceRequest.getSubject());

				// check if we already have this.

				// post this to the server if not exist.
			}
		}

		// Specimen
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();			

			if (resource instanceof Specimen) {
				Specimen specimen = (Specimen) resource;
				updateReference(specimen.getSubject());

				List<Reference> references = specimen.getParent();
				for (Reference reference : references) {
					updateReference(reference);
				}

				references = specimen.getRequest();
				for (Reference reference : references) {
					updateReference(reference);
				}

				String specimenId = null;
				MethodOutcome outcome; 
				if (specimen.getIdElement() != null && !specimen.getIdElement().isEmpty()) {
					outcome = client.update().resource(specimen).prettyPrint().encodedJson().execute();
				} else {
					outcome = client.create().resource(specimen).prettyPrint().encodedJson().execute();
				}

				OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
				if (oo == null) {
					specimenId = outcome.getId().getIdPart();
					referenceIds.put("Specimen/" + specimen.getIdElement().getIdPart(), "Specimen/" + specimenId);
				} else {
					throw new UnprocessableEntityException("Unable to create/update Specimen : " + specimen.getId(), oo);
				}
			}
		}

		// Observation
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();			

			if (resource instanceof Observation) {
				Observation observation = (Observation) resource;
				updateReference(observation.getSubject());
				updateReference(observation.getPerformerFirstRep());

				String observationId = null;
				MethodOutcome outcome;
				if (observation.getIdElement() != null && !observation.getIdElement().isEmpty()) {
					outcome = client.update().resource(observation).prettyPrint().encodedJson().execute();
				} else {
					outcome = client.create().resource(observation).prettyPrint().encodedJson().execute();
				}

				OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
				if (oo == null) {
					observationId = outcome.getId().getIdPart();
					referenceIds.put("Observation/" + observation.getIdElement().getIdPart(), "Observation/" + observationId);
				} else {
					throw new UnprocessableEntityException("Unable to create/update Observation : " + observation.getId(), oo);
				}
			}
		}

		// DocumentReference
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();			

			if (resource instanceof DocumentReference) {
				DocumentReference documentReference = (DocumentReference) resource;

				updateReference(documentReference.getSubject());
				String documentReferenceId = null;
				MethodOutcome outcome;
				if (documentReference.getIdElement() != null && !documentReference.getIdElement().isEmpty()) {
					outcome = client.update().resource(documentReference).prettyPrint().encodedJson().execute();
				} else {
					outcome = client.create().resource(documentReference).prettyPrint().encodedJson().execute();
				}

				OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
				if (oo == null) {
					documentReferenceId = outcome.getId().getIdPart();
					referenceIds.put("DocumentReference/" + documentReference.getIdElement().getIdPart(), "DocumentReference/" + documentReferenceId);
				} else {
					throw new UnprocessableEntityException("Unable to create/update DocumentReference : " + documentReference.getId(), oo);
				}

				// DocumentReference may have an attachment. Check and handle this here.
				List<DocumentReferenceContentComponent> contents = documentReference.getContent();
				for (DocumentReferenceContentComponent content : contents) {
					Attachment attachment = content.getAttachment();
					if (attachment != null && !attachment.isEmpty()) {
						String attachmentUrl = attachment.getUrl();
						// We are mapping the Binary/<id> portion only
						if (attachmentUrl != null && !attachmentUrl.isEmpty()) {
							int lastSlashIndex = attachmentUrl.lastIndexOf("/");
							if (lastSlashIndex != -1) {
								String attachmentId = attachmentUrl.substring(lastSlashIndex+1);
								String updateId = referenceIds.get("Binary/" + attachmentId);
								if (updateId != null && !updateId.isEmpty()) {
									String myUrl = System.getenv("INTERNAL_FHIR_REQUEST_URL");
									if (myUrl == null || myUrl.isEmpty()) {
										myUrl = "http://localhost:8080/fhir";
									}
									attachment.setUrl(myUrl + "/" + updateId);
								}
							}
						}
					}
				}
			}
		}

		// DiagnosticReport
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();			

			if (resource instanceof DiagnosticReport) {
				DiagnosticReport diagnosticReport = (DiagnosticReport) resource;

				updateReference(diagnosticReport.getSubject());
				updateReference(diagnosticReport.getEncounter());

				for (Reference reference : diagnosticReport.getPerformer()) {
					updateReference(reference);		
				}

				for (Reference reference : diagnosticReport.getResultsInterpreter()) {
					updateReference(reference);		
				}

				for (Reference reference : diagnosticReport.getSpecimen()) {
					updateReference(reference);		
				}

				for (Reference reference : diagnosticReport.getResult()) {
					updateReference(reference);		
				}

				for (Reference reference : diagnosticReport.getImagingStudy()) {
					updateReference(reference);		
				}

				for (Extension extension : diagnosticReport.getExtension()) {
					if ("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-agency-case-history-notes".equals(extension.getUrl())) {
						Reference reference = (Reference) extension.getValue();
						updateReference(reference);
					}
				}

				String diagnosticReportId = null;
				MethodOutcome outcome;
				if (diagnosticReport.getIdElement() != null && !diagnosticReport.getIdElement().isEmpty()) {
					outcome = client.update().resource(diagnosticReport).prettyPrint().encodedJson().execute();
				} else {
					outcome = client.create().resource(diagnosticReport).prettyPrint().encodedJson().execute();
				}

				OperationOutcome oo = (OperationOutcome) outcome.getOperationOutcome();
				if (oo == null) {
					diagnosticReportId = outcome.getId().getIdPart();
					referenceIds.put("DiagnosticReport/" + diagnosticReport.getIdElement().getIdPart(), "DiagnosticReport/" + diagnosticReportId);
				} else {
					throw new UnprocessableEntityException("Unable to create/update DiagnosticReport : " + diagnosticReport.getId(), oo);
				}
			}
		}

		// If we are here, then we do not have any errors. So, return null.
		return null;
	}

	@Operation(name = "$process-message")
	public Bundle processMessageOperation(@OperationParam(name = "content") Bundle theContent,
			@OperationParam(name = "async") BooleanType theAsync,
			@OperationParam(name = "response-url") UriType theUri) {

		if (theAsync != null && theAsync.booleanValue()) {
			ThrowFHIRExceptions.unprocessableEntityException("Asynchronous is not supported");
		}

		if (theUri != null && !theUri.isEmpty()) {
			ThrowFHIRExceptions.unprocessableEntityException("response-uri is not supported as async message is not supported");
		}

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

		// First save this bundle to fhirbase
		OperationUtil.createResource(client, BundleResourceProvider.getType(), theContent);

		Bundle responseBundle = new Bundle();
		responseBundle.setId(new IdType(BundleResourceProvider.getType(), UUID.randomUUID().toString()));
		responseBundle.setType(BundleType.MESSAGE);
		responseBundle.getMeta().addProfile("http://hl7.org/fhir/us/mdi/StructureDefinition/Bundle-message-tox-to-mdi");

		referenceIds = new HashMap<String, String>();
		patientIds = new ArrayList<String>();

		MessageHeader messageHeader = null;
		String originalMessageHeaderId = null;

		if (theContent.getType() == BundleType.MESSAGE) {
			List<BundleEntryComponent> entries = theContent.getEntry();

			// Evaluate the first entry, which must be MessageHeader
			if (entries != null && !entries.isEmpty() && entries.get(0).getResource() != null
					&& entries.get(0).getResource().getResourceType() == ResourceType.MessageHeader) {
				messageHeader = (MessageHeader) entries.get(0).getResource();
				originalMessageHeaderId = messageHeader.getIdElement().getIdPart();

				// We handle only MDI Toxicology Lab type.
				Type eventType = messageHeader.getEvent();
				if (eventType instanceof Coding) {
					Coding event = (Coding) eventType;
					if ("http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes".equals(event.getSystem())
						&& "tox-result-report".equals(event.getCode())) {
						
						
						// This is tox lab report. Resources are all to be added to the server.
						OperationOutcome oo = processPostResources(client, entries);
						if (oo != null) {
							throw new UnprocessableEntityException(FhirContext.forR4(), oo);
						}

						// update message.focus().
						List<Reference> references = messageHeader.getFocus();
						if (references.size() != 1) {
							oo = new OperationOutcome();
							OperationOutcomeIssueComponent ooIssue = new OperationOutcomeIssueComponent();
							ooIssue.setSeverity(IssueSeverity.ERROR);
							ooIssue.setCode(IssueType.INVALID);
							ooIssue.setDetails((new CodeableConcept()).setText("messageHeader.focus must have DiagnosticReport"));
							oo.addIssue(ooIssue);
							throw new UnprocessableEntityException(FhirContext.forR4(), oo);
						}
						updateReference(references.get(0));
						createMessageHeader(client, messageHeader);
					} else {
						ThrowFHIRExceptions.unprocessableEntityException("We currently support only observation-provided Message event");
					}
				} else {
					ThrowFHIRExceptions
							.unprocessableEntityException("We currently support only MessageHeader.eventCoding");
				}
			}
		} else {
			ThrowFHIRExceptions.unprocessableEntityException("The bundle must be a MESSAGE type");
		}

		MessageHeader responseMessageHeader = new MessageHeader();
		responseMessageHeader.getMeta().addProfile("https://fhir.org/fhir/us/mdi/StructureDefinition/MessageHeader-toxicology-to-mdi");
		responseMessageHeader.setEvent(new Coding("http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes", "tox-result-report", ""));
		responseMessageHeader.getResponse().setId(originalMessageHeaderId);
		responseMessageHeader.getResponse().setCode(ResponseType.OK);

		responseBundle.getEntryFirstRep().setFullUrl("MessageHeader/" + UUID.randomUUID());
		responseBundle.getEntryFirstRep().setResource(responseMessageHeader);
		
		return responseBundle;
	}
}
