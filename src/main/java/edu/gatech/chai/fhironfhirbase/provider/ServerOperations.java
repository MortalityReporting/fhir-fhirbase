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

import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.MessageHeader;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.codesystems.ObservationCategory;
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
import edu.gatech.chai.fhironfhirbase.utilities.CodeableConceptUtil;
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

		logger.debug("orginal id: " + originalId + " new id:" + newId);
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
		for (String patientId : patientIds) {
			// Check if we already have a composition for the same patient
			Bundle responseBundle = client.search().forResource(Composition.class)
					.where(Composition.SUBJECT.hasAnyOfIds(patientId))
					.and(Composition.TYPE.exactly().systemAndCode("http://loinc.org", "11502-2"))
					.returnBundle(Bundle.class).execute();

			Composition composition;
			boolean create = true;
			if (responseBundle.getTotal() > 0) {
				composition = (Composition) responseBundle.getEntryFirstRep().getResource();
				create = false; // we should update
			} else {
				// Create a composition for this batch write.
				composition = new Composition();
				composition
						.setType(new CodeableConcept(new Coding("http://loinc.org", "64297-5", "Death certificate")));
			}

			// Walk through the entries again and add the entries to composition
			SectionComponent sectionComponent = new SectionComponent();
			for (BundleEntryComponent entry : entries) {
				Resource resource = entry.getResource();
				if (resource instanceof Patient) {
					composition.setSubject(new Reference(resource.getIdElement()));
				}
				sectionComponent.addEntry(new Reference(resource));
			}

			composition.getSection().add(sectionComponent);
			MethodOutcome mo;
			if (create) {
				mo = client.create().resource(composition).encodedJson().prettyPrint().execute();
			} else {
				mo = client.update().resource(composition).encodedJson().prettyPrint().execute();
			}

			if (mo.getId() == null) {
				logger.debug("Failed to create a composition for the batch upload.");
			}
		}
	}
	
	private void createMessageHeader(IGenericClient client, MessageHeader messageHeader) {
		// We store all the messaging events.
		client.create().resource(messageHeader).encodedJson().prettyPrint().execute();
	}

	private OperationOutcome processPostResources(IGenericClient client, List<BundleEntryComponent> entries) {
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();

			// Do the patient first.
			String patientId = null;
			if (resource instanceof Patient) {
				Patient patient = (Patient) resource;
				for (Identifier identifier : patient.getIdentifier()) {
					// do search only on the case number.
//					CodeableConcept identifierType = identifier.getType();
//					if (identifierType.isEmpty()) {
//						continue;
//					}
//
//					Coding identifierCoding = identifierType.getCodingFirstRep();
//					if (identifierCoding.isEmpty()) {
//						continue;
//					}
//
//					if (!"urn:mdi:temporary:code".equalsIgnoreCase(identifierCoding.getSystem())
//							|| !"1000007".equalsIgnoreCase(identifierCoding.getCode())) {
//						continue;
//					}

					Bundle responseBundle = client
							.search().forResource(Patient.class).where(Patient.IDENTIFIER.exactly()
									.systemAndCode(identifier.getSystem(), identifier.getValue()))
							.returnBundle(Bundle.class).execute();

					int total = responseBundle.getTotal();
					if (total > 0) {
						patientId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
						break;
					}
				}

				if (patientId != null && !patientId.isEmpty()) {
					// We do not update the patient info from the lab report. So we just get
					// patient id and keep the information
					referenceIds.put(entry.getFullUrl(), "Patient/" + patientId);
					patientIds.add("Patient/" + patientId);
					patient.setId(patientId);
				} else {
					MethodOutcome outcome = client.create().resource(patient).prettyPrint().encodedJson().execute();
					if (outcome.getCreated()) {
						patientId = outcome.getId().getIdPart();
						referenceIds.put(entry.getFullUrl(), "Patient/" + patientId);
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
		}

		// Now, we re-loop the entries and take care of resources other than patient.
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
							observation.setId(observationId);
							break;
						}

						if (observationId != null && !observationId.isEmpty()) {
							break;
						}
					}

					if (observationId != null && !observationId.isEmpty()) {
						break;
					}
				}

				if (observationId != null && !observationId.isEmpty()) {
					MethodOutcome outcome = client.update().resource(observation).prettyPrint().encodedJson().execute();
					if (outcome.getId() == null || outcome.getId().isEmpty()) {
						return (OperationOutcome) outcome.getOperationOutcome();
					}
				} else {
					MethodOutcome outcome = client.create().resource(observation).prettyPrint().encodedJson().execute();
					if (outcome.getCreated()) {
						observationId = outcome.getId().getIdPart();
					} else {
						return (OperationOutcome) outcome.getOperationOutcome();
					}

				}
				referenceIds.put(entry.getFullUrl(), "Observation/" + observationId);

				observation.setId(observationId);
				entry.setFullUrl("Observation/" + observationId);
			}
		}

		// If we are here, then we do not have any errors. So, return null.
		return null;
	}

	@Operation(name = "$process-message")
	public Bundle ProcessMessageOperation(@OperationParam(name = "content") Bundle theContent,
			@OperationParam(name = "async") BooleanType theAsync,
			@OperationParam(name = "response-url") UriType theUri) {
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

		referenceIds = new HashMap<String, String>();
		MessageHeader messageHeader = null;

		if (theContent.getType() == BundleType.MESSAGE) {
			List<BundleEntryComponent> entries = theContent.getEntry();

			// Evaluate the first entry, which must be MessageHeader
			if (entries != null && entries.size() > 0 && entries.get(0).getResource() != null
					&& entries.get(0).getResource().getResourceType() == ResourceType.MessageHeader) {
				messageHeader = (MessageHeader) entries.get(0).getResource();

				// We handle observation-type.
				// TODO: Add other types later.
				Type eventType = messageHeader.getEvent();
				if (eventType instanceof Coding) {
					Coding event = eventType.castToCoding(eventType);
					Coding obsprovided = new Coding(ObservationCategory.LABORATORY.getSystem(),
							ObservationCategory.LABORATORY.toCode(), ObservationCategory.LABORATORY.getDisplay());
					if (CodeableConceptUtil.compareCodings(event, obsprovided) == 0) {
						// This is lab report. they are all to be added to the server.
						// We add resources that are focused.
						OperationOutcome oo = processPostResources(client, entries);
						if (oo != null) {
							throw new UnprocessableEntityException(FhirContext.forR4(), oo);
						}

						// update message.focus().
						List<Reference> references = messageHeader.getFocus();
						for (Reference reference : references) {
							updateReference(reference);
						}

						createMessageHeader(client, messageHeader);
//						createComposition(client, entries);
					} else {
						ThrowFHIRExceptions.unprocessableEntityException(
								"We currently support only observation-provided Message event");
					}
				} else {
					ThrowFHIRExceptions
							.unprocessableEntityException("We currently support only MessageHeader.eventCoding");
				}
			}
		} else {
			ThrowFHIRExceptions.unprocessableEntityException("The bundle must be a MESSAGE type");
		}

		return theContent;
	}
}
