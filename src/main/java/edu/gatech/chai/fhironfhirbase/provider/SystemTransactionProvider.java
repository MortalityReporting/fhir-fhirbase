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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DocumentReference;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryResponseComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Practitioner;
import org.hl7.fhir.r4.model.Procedure;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RelatedPerson;
import org.springframework.http.HttpStatus;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.ListResource;
import org.hl7.fhir.r4.model.ListResource.ListEntryComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Resource;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import edu.gatech.chai.fhironfhirbase.model.MyBundle;

public class SystemTransactionProvider {
	private static final Logger logger = LoggerFactory.getLogger(SystemTransactionProvider.class);

	private FhirContext ctx;
	private String caseNumber;
	private String patientId;
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
//						String resourceName = url[0];
//						String id = url[1];
//						Class<? extends Resource> resourceClass = getResourceClass(resourceName);
//						if (resourceClass == null || id == null || id.isEmpty()) {
//							response.setStatus(String.valueOf(HttpStatus.BAD_REQUEST.value())
//									+ HttpStatus.BAD_REQUEST.getReasonPhrase());
//						} else {
//							fhirbaseMapping.delete(resourceName.toLowerCase(), id, resourceClass);
//							response.setStatus(String.valueOf(HttpStatus.OK.value()));
//						}
					}
				} else {
					response.setStatus(String.valueOf(HttpStatus.BAD_REQUEST.value()));
				}

				entry.setRequest(null);
			}
		}
	}

	private void processPostPatient(IGenericClient client, List<BundleEntryComponent> entries) {
		for (BundleEntryComponent entry : entries) {
			BundleEntryResponseComponent response = entry.getResponse();
			if (response != null && !response.isEmpty()) {
				// We have already processed this.
				continue;
			}

			Resource resource = entry.getResource();
			if (resource instanceof Patient) {
				Patient patient = (Patient) resource;
				patientId = null;
				for (Identifier identifier : patient.getIdentifier()) {
					Bundle responseBundle = client
							.search().forResource(Patient.class).where(Patient.IDENTIFIER.exactly()
									.systemAndCode(identifier.getSystem(), identifier.getValue()))
							.returnBundle(Bundle.class).execute();

					int total = responseBundle.getTotal();
					if (total > 0) {
						patientId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
						caseNumber = identifier.getValue();

						System.out.println(">>>>>>>>>>>>>>>>" + caseNumber + ", patientId:" + patientId);
						break;
					}
				}

				if (patientId != null && !patientId.isEmpty()) {
					referenceIds.put(entry.getFullUrl(), "Patient/" + patientId);
					// Now we need to write this to fhirbase.
					patient.setIdElement(new IdType("Patient", patientId));
//					patient.setId("Patient/"+patientId);
					MethodOutcome outcome = client.update().resource(patient).execute();
					if (outcome.getId() != null && !outcome.getId().isEmpty()) {
						response.setStatus(
								String.valueOf(HttpStatus.OK.value()) + " " + HttpStatus.OK.getReasonPhrase());
						response.setLocation(PatientResourceProvider.getType() + "/" + patientId);

						entry.setFullUrl(client.getServerBase() + "Patient/" + patientId);
						if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
							entry.setResource((Resource) outcome.getResource());
						}
					} else {
						response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()) + " "
								+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
						entry.setResource(null);
					}
				} else {
					MethodOutcome outcome = client.create().resource(patient).prettyPrint().encodedJson().execute();
					if (outcome.getCreated()) {
						patientId = outcome.getId().getIdPart();
						referenceIds.put(entry.getFullUrl(), "Patient/" + patientId);
						response.setStatus(String.valueOf(HttpStatus.CREATED.value()) + " "
								+ HttpStatus.CREATED.getReasonPhrase());
						response.setLocation(PatientResourceProvider.getType() + "/" + patientId);

						entry.setFullUrl(client.getServerBase() + "Patient/" + patientId);
						if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
							entry.setResource((Resource) outcome.getResource());
						} else {
							patient.setId(patientId);
						}
					} else {
						response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()) + " "
								+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
						entry.setResource(null);
					}
				}

				entry.setRequest(null);
			}
		}
	}

	private void processPostPractitioner(IGenericClient client, List<BundleEntryComponent> entries) {
		for (BundleEntryComponent entry : entries) {
			BundleEntryResponseComponent response = entry.getResponse();
			if (response != null && !response.isEmpty()) {
				// We have already processed this.
				continue;
			}

			Resource resource = entry.getResource();
			if (resource instanceof Practitioner) {
				Practitioner practitioner = (Practitioner) resource;
				String practitionerId = null;
				for (Identifier identifier : practitioner.getIdentifier()) {
					Bundle responseBundle = client
							.search().forResource(Patient.class).where(Practitioner.IDENTIFIER.exactly()
									.systemAndCode(identifier.getSystem(), identifier.getValue()))
							.returnBundle(Bundle.class).execute();

					int total = responseBundle.getTotal();
					if (total > 0) {
						practitionerId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
						break;
					}
				}

				if (practitionerId != null && !practitionerId.isEmpty()) {
					referenceIds.put(entry.getFullUrl(), "Practitioner/" + practitionerId);

					// Now we need to write this to fhirbase.
					practitioner.setId(new IdType("Practitioner", practitionerId));
					MethodOutcome outcome = client.update().resource(practitioner).prettyPrint().encodedJson()
							.execute();
					if (outcome.getCreated()) {
						response.setStatus(
								String.valueOf(HttpStatus.OK.value()) + " " + HttpStatus.OK.getReasonPhrase());
						response.setLocation(PractitionerResourceProvider.getType() + "/" + practitionerId);

						entry.setFullUrl(client.getServerBase() + "Practitioner/" + practitionerId);
						if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
							entry.setResource((Resource) outcome.getResource());
						}
					} else {
						response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()) + " "
								+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
						entry.setResource(null);
					}
				} else {
					MethodOutcome outcome = client.create().resource(practitioner).prettyPrint().encodedJson()
							.execute();
					if (outcome.getCreated()) {
						practitionerId = outcome.getId().getIdPart();
						referenceIds.put(entry.getFullUrl(), "Practitioner/" + practitionerId);

						response.setStatus(String.valueOf(HttpStatus.CREATED.value()) + " "
								+ HttpStatus.CREATED.getReasonPhrase());
						response.setLocation(PractitionerResourceProvider.getType() + "/" + practitionerId);

						entry.setFullUrl(client.getServerBase() + "Practitioner/" + practitionerId);
						if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
							entry.setResource((Resource) outcome.getResource());
						}
						entry.setRequest(null);
					} else {
						response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()) + " "
								+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
						entry.setResource(null);
					}
				}

				entry.setRequest(null);
			}
		}
	}

	private void updateReference(Reference reference) {
		if (reference == null || reference.isEmpty())
			return;

		if (reference.getReferenceElement().isIdPartValid()) {
			String originalId = reference.getReferenceElement().getIdPart();
			String newId = referenceIds.get(originalId);

			if (newId != null && !newId.isEmpty()) {
				String[] resourceId = newId.split("/");
				if (resourceId.length == 2) {
					reference.setReferenceElement(new IdType(resourceId[0], resourceId[1]));
				} else {
					reference.setReferenceElement(new IdType(newId));
				}
			}
		}
	}

	private void processPost(IGenericClient client, List<BundleEntryComponent> entries) {
		for (BundleEntryComponent entry : entries) {
			BundleEntryResponseComponent response = entry.getResponse();
			if (response != null && !response.isEmpty()) {
				// We have already processed this.
				continue;
			}

			BundleEntryRequestComponent request = entry.getRequest();

			HTTPVerb requestMethod;
			if (request == null || request.isEmpty()) {
				requestMethod = HTTPVerb.POST;
			} else {
				requestMethod = request.getMethod();
			}

			if (requestMethod.equals(HTTPVerb.POST) || requestMethod.equals(HTTPVerb.PUT)
					|| requestMethod.equals(HTTPVerb.PATCH)) {
				response = entry.getResponse();
				Resource resource = entry.getResource();
				if (resource != null && !resource.isEmpty()) {
					if (resource instanceof Observation) {
						Observation res = (Observation) resource;
						updateReference(res.getSubject());
						updateReference(res.getPerformerFirstRep());
					} else if (resource instanceof Condition) {
						Condition res = (Condition) resource;
						updateReference(res.getSubject());
						updateReference(res.getAsserter());
					} else if (resource instanceof Procedure) {
						Procedure res = (Procedure) resource;
						updateReference(res.getSubject());
						updateReference(res.getAsserter());
					} else if (resource instanceof ListResource) {
						ListResource res = (ListResource) resource;
						for (ListEntryComponent listEntry : res.getEntry()) {
							updateReference(listEntry.getItem());
						}
					} else if (resource instanceof DocumentReference) {
						DocumentReference res = (DocumentReference) resource;
						updateReference(res.getSubject());
					} else if (resource instanceof RelatedPerson) {
						RelatedPerson res = (RelatedPerson) resource;
						updateReference(res.getPatient());
					}

					MethodOutcome outcome = client.create().resource(resource).prettyPrint().encodedJson().execute();
					if (outcome.getCreated() != null && outcome.getCreated()) {
						String newId = outcome.getId().getIdPart();
						if (newId != null && !newId.isEmpty()) {
							response.setStatus(String.valueOf(HttpStatus.CREATED.value()) + " "
									+ HttpStatus.CREATED.getReasonPhrase());
							response.setLocation(resource.getResourceType().toString() + "/" + newId);

							entry.setFullUrl(
									client.getServerBase() + resource.getResourceType().toString() + "/" + newId);
							if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
								entry.setResource((Resource) outcome.getResource());
							} else {
								resource.setId(newId);
							}
						} else {
							entry.setResource(null);
							response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value())
									+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
						}
					} else {
						entry.setResource(null);
						response.setStatus(String.valueOf(HttpStatus.BAD_REQUEST.value()) + " "
								+ HttpStatus.BAD_REQUEST.getReasonPhrase());
					}
				} else {
					entry.setResource(null);
					response.setStatus(String.valueOf(HttpStatus.BAD_REQUEST.value()) + " "
							+ HttpStatus.BAD_REQUEST.getReasonPhrase());
				}

				entry.setRequest(null);
			}
		}
	}

	private void createComposition(IGenericClient client, List<BundleEntryComponent> entries) {
		// Check if we already have a composition for the same patient
		Bundle responseBundle = client.search().forResource(Composition.class)
				.where(Composition.SUBJECT.hasAnyOfIds(patientId))
				.and(Composition.TYPE.exactly().systemAndCode("http://loinc.org", "64297-5")).returnBundle(Bundle.class)
				.execute();

		Composition composition;
		boolean create = true;
		if (responseBundle.getTotal() > 0) {
			composition = (Composition) responseBundle.getEntryFirstRep().getResource();
			create = false; // we should update
		} else {
			// Create a composition for this batch write.
			composition = new Composition();
			composition.setType(new CodeableConcept(new Coding("http://loinc.org", "64297-5", "Death certificate")));
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

//	private void processGet(String requestUrl, List<BundleEntryComponent> entries) {
//		// TODO: finish this up
//		IGenericClient client = ctx.newRestfulGenericClient(requestUrl);
//
//		for (BundleEntryComponent entry : entries) {
//			BundleEntryResponseComponent response = entry.getResponse();
//			if (response != null && !response.isEmpty()) {
//				// We have already processed this.
//				continue;
//			}
//
//			BundleEntryRequestComponent request = entry.getRequest();
//			HTTPVerb requestMethod = request.getMethod();
//			if (requestMethod.equals(HTTPVerb.GET) || requestMethod.equals(HTTPVerb.HEAD)) {
//				response = entry.getResponse();
//				Resource resource = entry.getResource();
//				if (resource != null && !resource.isEmpty()) {
//
//					String newId = null; // fhirbaseMapping.create(resource);
//					if (newId != null && !newId.isEmpty()) {
//						response.setStatus(
//								String.valueOf(HttpStatus.CREATED.value()) + " " + HttpStatus.CREATED.getReasonPhrase());
//						response.setLocation(resource.getResourceType().toString() + "/" + newId);
//					} else {
//						response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value())
//								+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
//					}
//				} else {
//					response.setStatus(
//							String.valueOf(HttpStatus.BAD_REQUEST.value()) + HttpStatus.BAD_REQUEST.getReasonPhrase());
//				}
//
//				entry.setRequest(null);
//			}
//		}
//	}

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

		String requestUrl = theRequest.getRequestURL().toString();
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
			processPostPatient(client, entries);
			processPostPractitioner(client, entries);
			processPost(client, entries);
//			processGet(theRequest.getRequestURL().toString(), entries);

			createComposition(client, entries);

			theBundle.setType(BundleType.BATCHRESPONSE);

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

}
