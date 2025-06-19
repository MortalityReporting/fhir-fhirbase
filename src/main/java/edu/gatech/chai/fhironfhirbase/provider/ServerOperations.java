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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.hl7.fhir.r4.model.HumanName;
import org.apache.commons.text.WordUtils;
import org.hibernate.hql.internal.ast.tree.IdentNode;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Composition.CompositionStatus;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.ContactPoint.ContactPointSystem;
import org.hl7.fhir.r4.model.DocumentReference.DocumentReferenceContentComponent;
import org.hl7.fhir.r4.model.MessageHeader.MessageDestinationComponent;
import org.hl7.fhir.r4.model.MessageHeader.MessageSourceComponent;
import org.hl7.fhir.r4.model.MessageHeader.ResponseType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
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
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UriType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.primitive.DateDt;
import ca.uhn.fhir.model.primitive.DateTimeDt;
import ca.uhn.fhir.model.primitive.StringDt;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.MDI.model.resource.BundleDocumentMDIDCR;
import edu.gatech.chai.MDI.model.resource.BundleMessageDeathCertificateReview;
import edu.gatech.chai.MDI.model.resource.CompositionMDIDCR;
import edu.gatech.chai.MDI.model.resource.MessageHeaderDCR;
import edu.gatech.chai.MDI.model.resource.util.CommonUtil;
import edu.gatech.chai.VRDR.model.DeathDate;
import edu.gatech.chai.VRDR.model.DeathLocation;
import edu.gatech.chai.VRDR.model.Decedent;
import edu.gatech.chai.VRDR.model.FuneralHome;
import edu.gatech.chai.fhironfhirbase.utilities.CodeableConceptUtil;
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

	private String makeFullUrl(Resource resource) {
		String fullUrl = resource.getIdPart();
		if (fullUrl == null || fullUrl.isEmpty()) {
			String idUuid = UUID.randomUUID().toString();
			fullUrl = "urn:uuid:" + idUuid;
			resource.setId(idUuid);
		} else {
			fullUrl = "urn:uuid:" + fullUrl;
		}

		return fullUrl;
	}

	@Operation(name = "$ccr-funeralhome")
	public Bundle dcrConstructOperation(
		@OperationParam(name = "submitterFirstName", min = 1, max = 1, type = StringDt.class) StringParam theSubmitterFirstName,
		@OperationParam(name = "submitterLastName", min = 1, max = 1, type = StringDt.class) StringParam theSubmitterLastName,
		@OperationParam(name = "submitterEmail", min = 1, max = 1, type = StringDt.class) StringParam theSubmitterEmail,
		@OperationParam(name = "targetUrl", min = 0, max = 1, type = StringDt.class) StringParam theTargetUrl,
		@OperationParam(name = "funeralHomeName", min = 1, max = 1, type = StringDt.class) StringParam theFuneralHomeName,
		@OperationParam(name = "funeralHomeAddrLine1", min = 1, max = 1, type = StringDt.class) StringParam theFuneralHomeAddrLine1,
		@OperationParam(name = "funeralHomeAddrLine2", min = 0, max = 1, type = StringDt.class) StringParam theFuneralHomeAddrLine2,
		@OperationParam(name = "funeralHomeAddrCity", min = 1, max = 1, type = StringDt.class) StringParam theFuneralHomeAddrCity,
		@OperationParam(name = "funeralHomeAddrState", min = 1, max = 1, type = StringDt.class) StringParam theFuneralHomeAddrState,
		@OperationParam(name = "funeralHomeAddrZip", min = 1, max = 1, type = StringDt.class) StringParam theFuneralHomeAddrZip,
		@OperationParam(name = "funeralHomeAddrCountry", min = 1, max = 1, type = StringDt.class) StringParam theFuneralHomeAddrCountry,
		@OperationParam(name = "funeralHomeAddrPhone", min = 1, max = 1, type = StringDt.class) StringParam theFuneralHomeAddrPhone,
		@OperationParam(name = "funeralHomeAddrFax", min = 0, max = 1, type = StringDt.class) StringParam theFuneralHomeAddrFax,
		@OperationParam(name = "dateOfDeath", min = 1, max = 1, type = DateDt.class) DateParam theDateOfDeath,
		@OperationParam(name = "placeOfDeath", min = 1, max = 1, type = StringDt.class) StringParam thePlaceOfDeath,
		@OperationParam(name = "placeOfDeathOther", min = 0, max = 1, type = StringDt.class) StringParam thePlaceOfDeathOther,
		@OperationParam(name = "placeOfDeathFacilityName", min = 1, max = 1, type = StringDt.class) StringParam thePlaceOfDeathFacilityName,
		@OperationParam(name = "placeOfDeathAddrLine1", min = 0, max = 1, type = StringDt.class) StringParam thePlaceOfDeathAddrLine1,
		@OperationParam(name = "placeOfDeathAddrLine2", min = 0, max = 1, type = StringDt.class) StringParam thePlaceOfDeathAddrLine2,
		@OperationParam(name = "placeOfDeathAddrCity", min = 0, max = 1, type = StringDt.class) StringParam thePlaceOfDeathAddrCity,
		@OperationParam(name = "placeOfDeathAddrState", min = 0, max = 1, type = StringDt.class) StringParam thePlaceOfDeathAddrState,
		@OperationParam(name = "placeOfDeathAddrZip", min = 0, max = 1, type = StringDt.class) StringParam thePlaceOfDeathAddrZip,
		@OperationParam(name = "placeOfDeathAddrCountry", min = 0, max = 1, type = StringDt.class) StringParam thePlaceOfDeathAddrCountry,
		@OperationParam(name = "decedentFirstName", min = 1, max = 1) StringParam theDecedentFirstName,
		@OperationParam(name = "decedentMiddleName", min = 0, max = 1, type = StringDt.class) StringParam theDecedentMiddleName,
		@OperationParam(name = "decedentLastName", min = 1, max = 1, type = StringDt.class) StringParam theDecedentLastName,
		@OperationParam(name = "decedentDateOfBirth", min = 1, max = 1, type = DateTimeDt.class) DateParam theDecedentDateOfBirth,
		@OperationParam(name = "decedentSex", min = 0, max = 1, type = StringDt.class) StringParam theDecedentSex,
		@OperationParam(name = "decedentRace", min = 1, max = 5, type = StringDt.class) StringAndListParam theDecedentRaces,
		@OperationParam(name = "decedentEthnicity", min = 0, max = 1, type = StringDt.class) StringParam theDecedentEthnicity) {

		// construct dcr-message bundle
		// construct Decedent
		Decedent decedent = new Decedent();
		String decedentRefUrl = makeFullUrl(decedent);
		
		Identifier decedentIdentifier = new Identifier();
		decedentIdentifier.setSystem("urn:raven:dcr");
		decedentIdentifier.setValue(UUID.randomUUID().toString());
		decedent.addIdentifier(decedentIdentifier);

		HumanName name = new HumanName();

		if (theDecedentLastName == null) { // required
			throw new FHIRException("decedentLastName must exist");
		}
		name.setFamily(theDecedentLastName.getValue());

		if (theDecedentFirstName == null) { // required
			throw new FHIRException("decedentFirstName must exist");
		}
		name.addGiven(theDecedentFirstName.getValue());

		if (theDecedentMiddleName != null) { // 0..*
			name.addGiven(theDecedentMiddleName.getValue());
		}
		decedent.addName(name);

		if (theDecedentDateOfBirth == null) { // required
			throw new FHIRException("decedentDateOfBirth must exist");
		}

		Date decedentDob = theDecedentDateOfBirth.getValue();
		decedent.setBirthDate(decedentDob);

		if (theDecedentSex != null) {
			String decedentSex = theDecedentSex.getValue();
			String decedentSexDisplay = WordUtils.capitalizeFully(decedentSex);
			Extension ext = new Extension();
			ext.setUrl("http://hl7.org/fhir/us/vrdr/StructureDefinition/NVSS-SexAtDeath");
			ext.setValue(new CodeableConcept(new Coding("http://hl7.org/fhir/administrative-gender", decedentSex, decedentSexDisplay)));
			decedent.addExtension(ext);
		}

		if (theDecedentRaces == null || theDecedentRaces.getValuesAsQueryTokens().size() > 5) {
			throw new FHIRException("decedentRaces must have cardinality of 1..5");
		}

		List<StringOrListParam> theDecedentAndRaces = theDecedentRaces.getValuesAsQueryTokens();
		for (StringOrListParam theDecedentOrRace : theDecedentAndRaces) {
			for (StringParam theDecedentRace : theDecedentOrRace.getValuesAsQueryTokens()) {
				Extension ext = new Extension("http://hl7.org/fhir/us/core/StructureDefinition/us-core-race");
				String raceCode = theDecedentRace.getValue();
				Coding raceCoding = CodeableConceptUtil.usCoreRaceConceptFromCode(raceCode);
				ext.addExtension("ombCategory", raceCoding);
				ext.addExtension("text", new StringType(raceCoding.getDisplay()));
				decedent.addExtension(ext);
			}
		}		

		if (theDecedentEthnicity != null) {
			String ethnicityCode = theDecedentEthnicity.getValue();
			Extension ext = new Extension("http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity");
			Coding ethnicityCoding = CodeableConceptUtil.usCoreEthnicityConceptFromCode(ethnicityCode);
			ext.addExtension("ombCategory", ethnicityCoding);
			ext.addExtension("text", new StringType(ethnicityCoding.getDisplay()));
			decedent.addExtension(ext);
		}

		// construct Funeral Home
		if (theFuneralHomeName == null || theFuneralHomeAddrLine1 == null || theFuneralHomeAddrCity == null || 
			theFuneralHomeAddrState == null || theFuneralHomeAddrZip == null || theFuneralHomeAddrCountry == null) {
			throw new FHIRException("funeralHomeName, funeralHomeAddrLine1, funeralHomeAddrCity, funeralHomeAddrState, funeralHomeAddrZip, funeralHomeAddrCountry must exist");
		}

		Address funeralAddress = new Address();
		funeralAddress.addLine(theFuneralHomeAddrLine1.getValue());

		if (theFuneralHomeAddrLine2 != null) {
			funeralAddress.addLine(theFuneralHomeAddrLine2.getValue());
		}

		funeralAddress.setCity(theFuneralHomeAddrCity.getValue());
		funeralAddress.setState(theFuneralHomeAddrState.getValue());
		funeralAddress.setPostalCode(theFuneralHomeAddrZip.getValue());
		funeralAddress.setCountry(theFuneralHomeAddrCountry.getValue());

		FuneralHome fh = new FuneralHome(theFuneralHomeName.getValue(), funeralAddress);

		if (theFuneralHomeAddrPhone == null) {
			throw new FHIRException("funeralHomeAddrPhone must exist");
		}
		ContactPoint cp = new ContactPoint();
		cp.setSystem(ContactPointSystem.PHONE);
		cp.setValue(theFuneralHomeAddrPhone.getValue());
		fh.addTelecom(cp);

		if (theFuneralHomeAddrFax != null) {
			cp = new ContactPoint();
			cp.setSystem(ContactPointSystem.FAX);
			cp.setValue(theFuneralHomeAddrFax.getValue());
			fh.addTelecom(cp);
		}

		fh.setActive(true);


		// Funeral Director (or staff) Info
		Practitioner funeralDirector = new Practitioner();
		HumanName fdName = new HumanName();

		if (theSubmitterLastName == null) {
			throw new FHIRException("submitterLastName must exist");
		}
		fdName.setFamily(theSubmitterLastName.getValue());

		if (theSubmitterFirstName == null) {
			throw new FHIRException("submitterFirstName must exist");
		}
		fdName.addGiven(theSubmitterFirstName.getValue());
		
		funeralDirector.addName(fdName);

		if (theSubmitterEmail == null) {
			throw new FHIRException("submitterEmail must exist");
		}
		funeralDirector.addTelecom((new ContactPoint()).setSystem(ContactPointSystem.EMAIL).setValue(theSubmitterEmail.getValue()));

		// Date of death with Place of Death
		if (theDateOfDeath == null) {
			throw new FHIRException("dateOfDeath must exist");
		}

		if (thePlaceOfDeath == null) {
			throw new FHIRException("placeOfDeath must exist");
		}

		Date dateOfDeath = theDateOfDeath.getValue();
		DeathDate deathDate = new DeathDate(new Date(), dateOfDeath, thePlaceOfDeath.getValue());
		deathDate.setSubject(new Reference(decedentRefUrl));

		if ("OTH".equals(thePlaceOfDeath.getValue()) && thePlaceOfDeathOther != null) {
			CodeableConcept pDvalue = deathDate.getPlaceOfDeathComponentValue();
			pDvalue.setText(thePlaceOfDeathOther.getValue());
		}

		// Death Location
		DeathLocation deathLocation = new DeathLocation();

		if (thePlaceOfDeathFacilityName == null) {
			throw new FHIRException("placeOfDeathFacilityName must exist");
		}

		deathLocation.setName(thePlaceOfDeathFacilityName.getValue());
		Address deathLocationAddress = new Address();
		if (thePlaceOfDeathAddrLine1 != null) {
			deathLocationAddress.addLine(thePlaceOfDeathAddrLine1.getValue());
		}

		if (thePlaceOfDeathAddrLine2 != null) {
			deathLocationAddress.addLine(thePlaceOfDeathAddrLine2.getValue());
		}

		if (thePlaceOfDeathAddrCity != null) {
			deathLocationAddress.setCity(thePlaceOfDeathAddrCity.getValue());
		}

		if (thePlaceOfDeathAddrState != null) {
			deathLocationAddress.setState(thePlaceOfDeathAddrState.getValue());
		}

		if (thePlaceOfDeathAddrZip != null) {
			deathLocationAddress.setPostalCode(thePlaceOfDeathAddrZip.getValue());
		}

		if (thePlaceOfDeathAddrCountry != null) {
			deathLocationAddress.setCountry(thePlaceOfDeathAddrCountry.getValue());
		}

		if (!deathLocationAddress.isEmpty()) {
			deathLocation.setAddress(deathLocationAddress);
		}

		// construct DCR composition
		CompositionMDIDCR compositionMdiDcr = new CompositionMDIDCR();
		CommonUtil.setUUID(compositionMdiDcr);

		compositionMdiDcr.setDate(new Date());
		compositionMdiDcr.setStatus(CompositionStatus.FINAL);
		compositionMdiDcr.setTitle("Cremation Clearance Request");

		// DCR Document Bundle
		Identifier dcrIdentifier = new Identifier();
		dcrIdentifier.setSystem("urn:raven:dcr");
		dcrIdentifier.setValue(UUID.randomUUID().toString());

		BundleDocumentMDIDCR dcrBundle = new BundleDocumentMDIDCR(dcrIdentifier, compositionMdiDcr);
		dcrBundle.setTimestamp(new Date());
		dcrBundle.getEntryFirstRep().setFullUrl("urn:uuid:" + compositionMdiDcr.getIdPart());

		// Add resources to the bundle and add any references here
		// DCR Decedent Domgraphic Sectin
		BundleEntryComponent decedentEntry = new BundleEntryComponent();
		decedentEntry.setResource(decedent);
		decedentEntry.setFullUrl(decedentRefUrl);
		dcrBundle.addEntry(decedentEntry);

		// This is a decedent resource, which we also need to set to composition.subject
		compositionMdiDcr.setSubject(new Reference(decedentRefUrl));
		SectionComponent demographicSection = compositionMdiDcr.createDecedentDemographicsSection();
		demographicSection.addEntry(new Reference(decedentRefUrl));

		// DCR Death Investigateion Section
		String deathDateRefUrl = makeFullUrl(deathDate);
		BundleEntryComponent deathDateEntry = new BundleEntryComponent();
		deathDateEntry.setResource(deathDate);
		deathDateEntry.setFullUrl(deathDateRefUrl);
		dcrBundle.addEntry(deathDateEntry);

		String deathLocationUrl = null;
		if (!deathLocation.isEmpty()) {
			deathLocationUrl = makeFullUrl(deathLocation);
			BundleEntryComponent deathLocationEntry = new BundleEntryComponent();
			deathLocationEntry.setResource(deathLocation);
			deathLocationEntry.setFullUrl(deathLocationUrl);
			dcrBundle.addEntry(deathLocationEntry);		
		}

		SectionComponent deathInvestigationSection = compositionMdiDcr.createDeathInvestigationSection();
		deathInvestigationSection.addEntry(new Reference(deathDateRefUrl));
		if (deathLocationUrl != null) {
			deathInvestigationSection.addEntry(new Reference(deathLocationUrl));
		}

		// DCR Cremation Clearance Info section
		String ccrRefUrl = makeFullUrl(fh);
		BundleEntryComponent ccrInfoEntry = new BundleEntryComponent();
		ccrInfoEntry.setResource(fh);
		ccrInfoEntry.setFullUrl(ccrRefUrl);
		dcrBundle.addEntry(ccrInfoEntry);

		SectionComponent cremationClearanceInfoSection = compositionMdiDcr.createCreamationClearanceInfoSection();
		cremationClearanceInfoSection.addEntry(new Reference(ccrRefUrl));

		// Any other stuff.
		String submitterUrl = makeFullUrl(funeralDirector);
		BundleEntryComponent submitterEntry = new BundleEntryComponent();
		submitterEntry.setResource(funeralDirector);
		submitterEntry.setFullUrl(submitterUrl);
		dcrBundle.addEntry(submitterEntry);
		compositionMdiDcr.addAuthor(new Reference(submitterUrl));

		// First create a message header
		MessageHeaderDCR mhDcr = new MessageHeaderDCR();
		mhDcr.setReason(new CodeableConcept(new Coding(CommonUtil.deathCertificateReviewValuesetURL, "CREM_C_REQ", "Cremation Clearance Request")));

		if (theTargetUrl != null) {
			mhDcr.addDestination(new MessageDestinationComponent().setEndpoint(theTargetUrl.getValue()));
		}
		mhDcr.setSource(new MessageSourceComponent().setEndpoint("http://raven.icl.gtri.org/mdi-fhir-server/fhir"));
		// String dcrBundleRefUrl = "urn:uuid:" + UUID.randomUUID().toString();
		// mhDcr.addFocus(new Reference(dcrBundleRefUrl));

		// Create Message Bundle for DCR Document Bundle
		BundleMessageDeathCertificateReview dcrMessageBundle = new BundleMessageDeathCertificateReview(BundleType.MESSAGE, mhDcr);
		String mhRefUrl = makeFullUrl(mhDcr);
		BundleEntryComponent mhDcrEntry = dcrMessageBundle.getEntryFirstRep();
        mhDcrEntry.setFullUrl(mhRefUrl);

		// Add DCR bundle to focus and add to the entry
		BundleEntryComponent bDcrEntry = new BundleEntryComponent();
        String bDcrRefUrl = makeFullUrl(dcrBundle);
        bDcrEntry.setFullUrl(bDcrRefUrl);
        bDcrEntry.setResource(dcrBundle);
        dcrMessageBundle.addEntry(bDcrEntry);

        mhDcr.addFocus(new Reference(bDcrRefUrl));

		return dcrMessageBundle;
	}
}
