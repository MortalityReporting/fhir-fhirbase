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
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
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

		patientIds = new ArrayList<String>();
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
		client.create().resource(messageHeader).encodedJson().prettyPrint().execute();
	}

	private OperationOutcome processPostResources(IGenericClient client, List<BundleEntryComponent> entries) {

		// Patient and Binary
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();

			// Do the patient first.
			String patientId = null;
			if (resource instanceof Patient) {
				Patient patient = (Patient) resource;
				for (Identifier identifier : patient.getIdentifier()) {
					Bundle responseBundle = client
							.search().forResource(Patient.class).where(Patient.IDENTIFIER.exactly()
									.systemAndCode(identifier.getSystem(), identifier.getValue()))
							.returnBundle(Bundle.class).execute();

					int total = responseBundle.getTotal();
					if (total > 0) {
						patientId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
						if (patientId == null || patientId.isEmpty()) {
							logger.error("Found exising Patient. But patientId is null or empty");
						}
						break;
					}
				}

				// Toxicology does not have the identifier that we are using for Raven. Add it here.
				Identifier ravenIdentifier = new Identifier();
				Coding ravenIdCoding = new Coding ("urn:mdi:temporary:code", "1000007", "Case Number");
				CodeableConcept ravenIdCodeable = new CodeableConcept();
				ravenIdCodeable.addCoding(ravenIdCoding);
				ravenIdentifier.setType(ravenIdCodeable);
				ravenIdentifier.setSystem("urn:mdi:tox:connectathon");
				ravenIdentifier.setValue("000001");
				patient.addIdentifier(ravenIdentifier);

				if (patientId != null && !patientId.isEmpty()) {
					// We do not update the patient info from the lab report. So we just get
					// patient id and keep the information
					referenceIds.put("Patient/" + patient.getIdElement().getIdPart(), "Patient/" + patientId);
					patientIds.add("Patient/" + patientId);
					patient.setId(patientId);
					MethodOutcome outcome = client.update().resource(patient).prettyPrint().encodedJson().execute();
					if (outcome.getId() == null || outcome.getId().isEmpty()) {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				} else {
					MethodOutcome outcome = client.create().resource(patient).prettyPrint().encodedJson().execute();
					if (outcome.getCreated().booleanValue()) {
						patientId = outcome.getId().getIdPart();
						referenceIds.put("Patient/" + patient.getId(), "Patient/" + patientId);
						patientIds.add("Patient/" + patientId);

						if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
							entry.setResource((Resource) outcome.getResource());
						} else {
							patient.setId(patientId);
						}
					} else {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				}
				entry.setFullUrl("Patient/" + patientId);
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

		// We can't really do the search for existing ones to update. We just delete them all 
		// and repopulate. We do not delete Practitioner though...

		for (String pid : patientIds) {
			IdType subjectIdType = new IdType("Patient", pid);

			// Delete Specimens
			Bundle currentBundles = client.search().forResource(Specimen.class).where(Specimen.SUBJECT.hasId(subjectIdType)).returnBundle(Bundle.class).execute();
			if (currentBundles.getTotal() > 0) {
				List<BundleEntryComponent> currentEntries = currentBundles.getEntry();
				for (BundleEntryComponent currentEntry : currentEntries) {
					Specimen currentResource = (Specimen) currentEntry.getResource();
					client.delete().resourceById(currentResource.getIdElement()).execute();
				}
			}
	
			// Delete Observations
			currentBundles = client.search().forResource(Observation.class).where(Observation.SUBJECT.hasId(subjectIdType)).returnBundle(Bundle.class).execute();
			if (currentBundles.getTotal() > 0) {
				List<BundleEntryComponent> currentEntries = currentBundles.getEntry();
				for (BundleEntryComponent currentEntry : currentEntries) {
					Observation currentResource = (Observation) currentEntry.getResource();
					client.delete().resourceById(currentResource.getIdElement()).execute();
				}
			}

			// Delete DocumentReference
			currentBundles = client.search().forResource(DocumentReference.class).where(DocumentReference.SUBJECT.hasId(subjectIdType)).returnBundle(Bundle.class).execute();
			if (currentBundles.getTotal() > 0) {
				List<BundleEntryComponent> currentEntries = currentBundles.getEntry();
				for (BundleEntryComponent currentEntry : currentEntries) {
					DocumentReference currentResource = (DocumentReference) currentEntry.getResource();
					client.delete().resourceById(currentResource.getIdElement()).execute();
				}
			}

			// Delete DiagnosticReport
			currentBundles = client.search().forResource(DiagnosticReport.class).where(DiagnosticReport.SUBJECT.hasId(subjectIdType)).returnBundle(Bundle.class).execute();
			if (currentBundles.getTotal() > 0) {
				List<BundleEntryComponent> currentEntries = currentBundles.getEntry();
				for (BundleEntryComponent currentEntry : currentEntries) {
					DiagnosticReport currentResource = (DiagnosticReport) currentEntry.getResource();
					client.delete().resourceById(currentResource.getIdElement()).execute();
				}
			}
		}

		// Practitioner
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();

			if (resource instanceof Practitioner) {
				Practitioner practitioner = (Practitioner) resource;
				String practitionerId = null;
				for (Identifier identifier : practitioner.getIdentifier()) {
					Bundle responseBundle = client
							.search().forResource(Practitioner.class).where(Practitioner.IDENTIFIER.exactly()
									.systemAndCode(identifier.getSystem(), identifier.getValue()))
							.returnBundle(Bundle.class).execute();

					int total = responseBundle.getTotal();
					if (total > 0) {
						practitionerId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
						break;
					}
				}

				if (practitionerId != null && !practitionerId.isEmpty()) {
					// Now we need to write this to fhirbase.
					referenceIds.put("Practitioner/" + practitioner.getIdElement().getIdPart(), "Practitioner/" + practitionerId);
					MethodOutcome outcome = client.update().resource(practitioner).prettyPrint().encodedJson().execute();
					if (outcome.getId() == null || outcome.getId().isEmpty()) {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				} else {
					MethodOutcome outcome = client.create().resource(practitioner).prettyPrint().encodedJson().execute();
					if (outcome.getCreated().booleanValue()) {
						practitionerId = outcome.getId().getIdPart();
						referenceIds.put("Practitioner/" + practitioner.getIdElement().getIdPart(), "Practitioner/" + practitionerId);
					} else {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				}

				practitioner.setId(practitionerId);
				entry.setFullUrl("Practitioner/" + practitionerId);
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

				// check if we already have this using accessIdentifier.
				Identifier accessionIdentifier = specimen.getAccessionIdentifier();
				String specimenId = "";
				if (accessionIdentifier != null && !accessionIdentifier.isEmpty()) {
					for (String pId : patientIds) {
						Bundle responseBundle = client.search().forResource(Specimen.class)
							.where(Specimen.SUBJECT.hasId(pId))
							.and(Specimen.ACCESSION.exactly().systemAndCode(accessionIdentifier.getSystem(), accessionIdentifier.getValue()))
							.returnBundle(Bundle.class).execute();
	
						int total = responseBundle.getTotal();
						if (total > 0) {
							specimenId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
							break;
						}
					}
				}

				if (specimenId != null && !specimenId.isEmpty()) {
					referenceIds.put("Specimen/" + specimen.getIdElement().getIdPart(), "Specimen/" + specimenId);
					MethodOutcome outcome = client.update().resource(specimen).prettyPrint().encodedJson().execute();
					if (outcome.getId() == null || outcome.getId().isEmpty()) {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				} else {
					MethodOutcome outcome = client.create().resource(specimen).prettyPrint().encodedJson().execute();
					if (outcome.getCreated().booleanValue()) {
						specimenId = outcome.getId().getIdPart();
						referenceIds.put("Specimen/" + specimen.getIdElement().getIdPart(), "Specimen/" + specimenId);
					} else {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				}

				specimen.setId(specimenId);
				entry.setFullUrl("Specimen/" + specimenId);
			}
		}

		// Observation
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();			

			if (resource instanceof Observation) {
				Observation observation = (Observation) resource;
				updateReference(observation.getSubject());
				updateReference(observation.getPerformerFirstRep());

				String observationId = "";
				for (String pId : patientIds) {
					CodeableConcept obsCode = observation.getCode();
					List<Coding> obsCodings = obsCode.getCoding();
					for (Coding obsCoding : obsCodings) {
						String theSystem = obsCoding.getSystem();
						String theCode = obsCoding.getCode();
						DateTimeType theDateType = observation.getEffectiveDateTimeType();
						if (theDateType == null || theDateType.isEmpty()) {
							break;
						}

						Date theDate = theDateType.getValue();
						Bundle responseBundle = client.search().forResource(Observation.class)
								.where(Observation.SUBJECT.hasId(pId))
								.and(Observation.CODE.exactly().systemAndCode(theSystem, theCode))
								.and(Observation.DATE.exactly().second(theDate)).returnBundle(Bundle.class).execute();

						int total = responseBundle.getTotal();
						if (total > 0) {
							observationId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
							break;
						}
					}

					if (observationId != null && !observationId.isEmpty()) {
						break;
					}
				}

				if (observationId != null && !observationId.isEmpty()) {
					referenceIds.put("Observation/" + observation.getIdElement().getIdPart(), "Observation/" + observationId);
					MethodOutcome outcome = client.update().resource(observation).prettyPrint().encodedJson().execute();
					if (outcome.getId() == null || outcome.getId().isEmpty()) {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				} else {
					MethodOutcome outcome = client.create().resource(observation).prettyPrint().encodedJson().execute();
					if (outcome.getCreated().booleanValue()) {
						observationId = outcome.getId().getIdPart();
						referenceIds.put("Observation/" + observation.getIdElement().getIdPart(), "Observation/" + observationId);
					} else {
						return (OperationOutcome) outcome.getOperationOutcome();
					}

				}

				observation.setId(observationId);
				entry.setFullUrl("Observation/" + observationId);
			}
		}

		// DocumentReference
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();			

			if (resource instanceof DocumentReference) {
				DocumentReference documentReference = (DocumentReference) resource;

				updateReference(documentReference.getSubject());

				String documentReferenceId = null;
				for (Identifier identifier : documentReference.getIdentifier()) {
					Bundle responseBundle = client
							.search().forResource(DocumentReference.class).where(DocumentReference.IDENTIFIER.exactly()
									.systemAndCode(identifier.getSystem(), identifier.getValue()))
							.returnBundle(Bundle.class).execute();

					int total = responseBundle.getTotal();
					if (total > 0) {
						documentReferenceId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
						break;
					}
				}

				if (documentReferenceId != null && !documentReferenceId.isEmpty()) {
					// Now we need to write this to fhirbase.
					referenceIds.put("DocumentReference/" + documentReference.getIdElement().getIdPart(), "DocumentReference/" + documentReferenceId);
					MethodOutcome outcome = client.update().resource(documentReference).prettyPrint().encodedJson().execute();
					if (outcome.getId() == null || outcome.getId().isEmpty()) {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				} else {
					MethodOutcome outcome = client.create().resource(documentReference).prettyPrint().encodedJson().execute();
					if (outcome.getCreated().booleanValue()) {
						documentReferenceId = outcome.getId().getIdPart();
						referenceIds.put("DocumentReference/" + documentReference.getIdElement().getIdPart(), "DocumentReference/" + documentReferenceId);
					} else {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				}

				documentReference.setId(documentReferenceId);

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
				entry.setFullUrl("DocumentReference/" + documentReferenceId);				
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
				for (Identifier identifier : diagnosticReport.getIdentifier()) {
					updateReference(identifier.getAssigner());

					if (diagnosticReportId == null) {
						Bundle responseBundle = client
								.search().forResource(DiagnosticReport.class).where(DiagnosticReport.IDENTIFIER.exactly()
										.systemAndCode(identifier.getSystem(), identifier.getValue()))
								.returnBundle(Bundle.class).execute();

						int total = responseBundle.getTotal();
						if (total > 0) {
							diagnosticReportId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
						}
					}
				}

				if (diagnosticReportId != null && !diagnosticReportId.isEmpty()) {
					// Now we need to write this to fhirbase.
					referenceIds.put("DiagnosticReport/" + diagnosticReport.getIdElement().getIdPart(), "DiagnosticReport/" + diagnosticReportId);
					MethodOutcome outcome = client.update().resource(diagnosticReport).prettyPrint().encodedJson().execute();
					if (outcome.getId() == null || outcome.getId().isEmpty()) {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				} else {
					MethodOutcome outcome = client.create().resource(diagnosticReport).prettyPrint().encodedJson().execute();
					if (outcome.getCreated().booleanValue()) {
						diagnosticReportId = outcome.getId().getIdPart();
						referenceIds.put("DiagnosticReport/" + diagnosticReport.getIdElement().getIdPart(), "DiagnosticReport/" + diagnosticReportId);
					} else {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				}

				diagnosticReport.setId(diagnosticReportId);
				entry.setFullUrl("DiagnosticReport/" + diagnosticReportId);
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

		Bundle responseBundle = new Bundle();
		responseBundle.setType(BundleType.MESSAGE);
		responseBundle.getMeta().addProfile("http://hl7.org/fhir/us/mdi/StructureDefinition/Bundle-message-tox-to-mdi");

		referenceIds = new HashMap<String, String>();
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
					Coding event = eventType.castToCoding(eventType);
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
