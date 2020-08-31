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
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Practitioner;
import org.hl7.fhir.r4.model.Procedure;
import org.hl7.fhir.r4.model.Procedure.ProcedurePerformerComponent;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RelatedPerson;
import org.springframework.http.HttpStatus;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.ListResource;
import org.hl7.fhir.r4.model.ListResource.ListEntryComponent;
import org.hl7.fhir.r4.model.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Resource;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import edu.gatech.chai.fhironfhirbase.model.MyBundle;
import edu.gatech.chai.fhironfhirbase.utilities.MdiProfileUtil;

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
					// do search only on the case number.
					CodeableConcept identifierType = identifier.getType();
					if (identifierType.isEmpty()) {
						continue;
					}

					Coding identifierCoding = identifierType.getCodingFirstRep();
					if (identifierCoding.isEmpty()) {
						continue;
					}

					if (!"urn:mdi:temporary:code".equalsIgnoreCase(identifierCoding.getSystem())
							|| !"1000007".equalsIgnoreCase(identifierCoding.getCode())) {
						continue;
					}

					Bundle responseBundle = client
							.search().forResource(Patient.class).where(Patient.IDENTIFIER.exactly()
									.systemAndCode(identifier.getSystem(), identifier.getValue()))
							.returnBundle(Bundle.class).execute();

					int total = responseBundle.getTotal();
					if (total > 0) {
						patientId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
						caseNumber = identifier.getValue();

						logger.debug(">>>>>>>>>>>>>>>>" + caseNumber + ", patientId:" + patientId);
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
					referenceIds.put(entry.getFullUrl(), "Practitioner/" + practitionerId);

					// Now we need to write this to fhirbase.
					practitioner.setId(new IdType("Practitioner", practitionerId));
					MethodOutcome outcome = client.update().resource(practitioner).prettyPrint().encodedJson()
							.execute();
					if (outcome.getId() != null && !outcome.getId().isEmpty()) {
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

	private void processPostLocation(IGenericClient client, List<BundleEntryComponent> entries) {
		for (BundleEntryComponent entry : entries) {
			BundleEntryResponseComponent response = entry.getResponse();
			if (response != null && !response.isEmpty()) {
				// We have already processed this.
				continue;
			}

			Resource resource = entry.getResource();
			if (resource instanceof Location) {
				Location location = (Location) resource;
				String locationId = null;

				MethodOutcome outcome = client.create().resource(location).prettyPrint().encodedJson().execute();
				if (outcome.getCreated()) {
					locationId = outcome.getId().getIdPart();
					referenceIds.put(entry.getFullUrl(), "Location/" + locationId);

					response.setStatus(
							String.valueOf(HttpStatus.CREATED.value()) + " " + HttpStatus.CREATED.getReasonPhrase());
					response.setLocation(LocationResourceProvider.getType() + "/" + locationId);

					entry.setFullUrl(client.getServerBase() + "Location/" + locationId);
					if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
						entry.setResource((Resource) outcome.getResource());
					}
				} else {
					response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()) + " "
							+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
				}

				entry.setRequest(null);
			}
		}
	}
	
	private void processPostCondition(IGenericClient client, List<BundleEntryComponent> entries) {
		// It's hard to match the condition. So, we will clear all conditions.
		Bundle rConditionB = client.search().forResource(Condition.class).where(Condition.SUBJECT.hasId(patientId)).returnBundle(Bundle.class).execute();
		if (rConditionB.getTotal() > 0) {
			List<BundleEntryComponent> rConditions = rConditionB.getEntry();
			for (BundleEntryComponent rConditionComp : rConditions) {
				Condition rCond = (Condition) rConditionComp.getResource();
				client.delete().resourceById(rCond.getIdElement()).execute();
			}
		}
		
		for (BundleEntryComponent entry : entries) {
			BundleEntryResponseComponent response = entry.getResponse();
			if (response != null && !response.isEmpty()) {
				// We have already processed this.
				continue;
			}

			Resource resource = entry.getResource();
			if (resource instanceof Condition) {
				Condition condition = (Condition) resource;
				updateReference(condition.getSubject());
				updateReference(condition.getAsserter());
				
				MethodOutcome outcome = client.create().resource(condition).prettyPrint().encodedJson().execute();
				if (outcome.getCreated()) {
					String conditionId = outcome.getId().getIdPart();
					referenceIds.put(entry.getFullUrl(), "Condition/" + conditionId);

					response.setStatus(
							String.valueOf(HttpStatus.CREATED.value()) + " " + HttpStatus.CREATED.getReasonPhrase());
					response.setLocation(ConditionResourceProvider.getType() + "/" + conditionId);

					entry.setFullUrl(client.getServerBase() + "Condition/" + conditionId);
					if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
						entry.setResource((Resource) outcome.getResource());
					}
				} else {
					response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()) + " "
							+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
				}

				entry.setRequest(null);
			}
		}
	}

	private void updateReference(Reference reference) {
		if (reference == null || reference.isEmpty())
			return;

		String originalId = reference.getReferenceElement().getIdPart();
		String newId = referenceIds.get(originalId);

		logger.debug("new id:"+newId);
		if (newId != null && !newId.isEmpty()) {
			String[] resourceId = newId.split("/");
			if (resourceId.length == 2) {
				reference.setReferenceElement(new IdType(resourceId[0], resourceId[1]));
			} else {
				reference.setReferenceElement(new IdType(newId));
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
			
			String originalFullUrl = entry.getFullUrl();

			BundleEntryRequestComponent request = entry.getRequest();

			HTTPVerb requestMethod;
			if (request == null || request.isEmpty()) {
				requestMethod = HTTPVerb.POST;
			} else {
				requestMethod = request.getMethod();
			}

			if (requestMethod.equals(HTTPVerb.POST) || requestMethod.equals(HTTPVerb.PUT)) {
				response = entry.getResponse();
				Resource resource = entry.getResource();
				if (resource != null && !resource.isEmpty()) {
					String resourceId = null;
					
					if (resource instanceof Composition) {
						// Composition must be handled separately. 
						continue;
					}
					
					if (resource instanceof Observation) {
						Observation res = (Observation) resource;
						updateReference(res.getSubject());
						updateReference(res.getPerformerFirstRep());
						// Some of observations have an extension to
						// include location. We need to update this.
						List<Extension> extensions = res.getExtension();
						for (Extension extension : extensions) {
							String extUrl = extension.getUrl();
							if ("http://hl7.org/fhir/us/vrdr/StructureDefinition/VRDR-InjuryLocation"
									.equalsIgnoreCase(extUrl)
									|| "http://hl7.org/fhir/us/vrdr/StructureDefinition/VRDR-Disposition-Location"
											.equalsIgnoreCase(extUrl)
									|| "http://hl7.org/fhir/us/vrdr/StructureDefinition/VRDR-Death-Location"
											.equalsIgnoreCase(extUrl)) {
								// get valueReference and update it.
								updateReference((Reference) extension.getValue());
							}
						}

						// Check if this is singleton resource
						Coding myCoding;
						if ((myCoding = MdiProfileUtil.isSingletonResource(resource)) != null) {
							// We need to search and get an existing resource ID for this type of resource.
							Bundle responseBundle = client.search().forResource(Observation.class)
									.where(Observation.CODE.exactly().codings(myCoding))
									.and(Observation.PATIENT.hasId(patientId)).returnBundle(Bundle.class).execute();
							int total = responseBundle.getTotal();
							if (total > 0) {
								resourceId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
							}
						}
//					} else if (resource instanceof Condition) {
//						Condition res = (Condition) resource;
//						updateReference(res.getSubject());
//						updateReference(res.getAsserter());
					} else if (resource instanceof Procedure) {
						Procedure res = (Procedure) resource;
						updateReference(res.getSubject());
						ProcedurePerformerComponent performer = res.getPerformerFirstRep();
						if (performer != null && !performer.isEmpty()) {
							updateReference(performer.getActor());
						}
						updateReference(res.getAsserter());
						

						// Check if this is singleton resource
//						Coding myCoding;
//						if ((myCoding = MdiProfileUtil.isSingletonResource(resource)) != null) {
							// We need to search and get an existing resource ID for this type of resource.
//							Bundle responseBundle = client.search().forResource(Procedure.class)
//									.where(Procedure.CODE.exactly().codings(myCoding))
//									.and(Procedure.PATIENT.hasId(patientId)).returnBundle(Bundle.class).execute();
//							int total = responseBundle.getTotal();
//							if (total > 0) {
//								resourceId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
//								break;
//							}
						if (MdiProfileUtil.isSingletonResource(resource) != null) {
							for (Identifier identifier : res.getIdentifier()) {
								Bundle responseBundle = client
										.search().forResource(Procedure.class).where(Procedure.IDENTIFIER.exactly()
												.systemAndCode(identifier.getSystem(), identifier.getValue()))
										.returnBundle(Bundle.class).execute();

								int total = responseBundle.getTotal();
								if (total > 0) {
									resourceId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
									break;
								}
							}					
						}
					} else if (resource instanceof ListResource) {
						// Delete current ListResources in the server.
						Bundle rListResourceB = client.search().forResource(ListResource.class).where(ListResource.SUBJECT.hasId(patientId)).returnBundle(Bundle.class).execute();
						if (rListResourceB.getTotal() > 0) {
							List<BundleEntryComponent> rListResources = rListResourceB.getEntry();
							for (BundleEntryComponent rListResourceComp : rListResources) {
								ListResource rList = (ListResource) rListResourceComp.getResource();
								client.delete().resourceById(rList.getIdElement()).execute();
							}
						}

						ListResource res = (ListResource) resource;
						updateReference(res.getSubject());
						updateReference(res.getSource());
						for (ListEntryComponent listEntry : res.getEntry()) {
							logger.debug("list item before:"+listEntry.getItem().getReferenceElement().getValue());
							updateReference(listEntry.getItem());
							logger.debug("list item after:"+listEntry.getItem().getReferenceElement().getValue());
						}
					} else if (resource instanceof DocumentReference) {
						DocumentReference res = (DocumentReference) resource;
						updateReference(res.getSubject());
					} else if (resource instanceof RelatedPerson) {
						RelatedPerson res = (RelatedPerson) resource;
						updateReference(res.getPatient());
					} else if (resource instanceof Organization) {
						// For organization, we need to see if we have an existing one before
						// we create.
						Organization res = (Organization) resource;
						for (Identifier identifier : res.getIdentifier()) {
							Bundle responseBundle = client
									.search().forResource(Organization.class).where(Organization.IDENTIFIER.exactly()
											.systemAndCode(identifier.getSystem(), identifier.getValue()))
									.returnBundle(Bundle.class).execute();

							int total = responseBundle.getTotal();
							if (total > 0) {
								resourceId = responseBundle.getEntryFirstRep().getResource().getIdElement().getIdPart();
								break;
							}
						}

					}

					MethodOutcome outcome;
					if (resourceId != null) {
						logger.debug("Existing Resource found with "+resource.getResourceType().toString()+"/"+resourceId);
						resource.setId(new IdType(resource.getResourceType().toString(), resourceId));
						outcome = client.update().resource(resource).prettyPrint().encodedJson().execute();
						if (outcome.getId() != null && !outcome.getId().isEmpty()) {
							response.setStatus(
									String.valueOf(HttpStatus.OK.value()) + " " + HttpStatus.OK.getReasonPhrase());
							response.setLocation(resource.getResourceType().toString() + "/" + resourceId);

							entry.setFullUrl(
									client.getServerBase() + resource.getResourceType().toString() + "/" + resourceId);
							if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
								entry.setResource((Resource) outcome.getResource());
							}
						} else {
							response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value())
									+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
						}
					} else {
						if (resource instanceof ListResource) {
							logger.debug("before create:"+((ListResource)resource).getEntryFirstRep().getItem().getReferenceElement().getResourceType());
						}
						outcome = client.create().resource(resource).prettyPrint().encodedJson().execute();
						if (outcome.getCreated() != null && outcome.getCreated()) {
							resourceId = outcome.getId().getIdPart();
							if (resourceId != null && !resourceId.isEmpty()) {
								response.setStatus(String.valueOf(HttpStatus.CREATED.value()) + " "
										+ HttpStatus.CREATED.getReasonPhrase());
								response.setLocation(resource.getResourceType().toString() + "/" + resourceId);

								entry.setFullUrl(
										client.getServerBase() + resource.getResourceType().toString() + "/" + resourceId);
								if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
									entry.setResource((Resource) outcome.getResource());
								} else {
									resource.setId(resourceId);
								}
							} else {
								response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value())
										+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
							}
						} else {
							response.setStatus(String.valueOf(HttpStatus.BAD_REQUEST.value()) + " "
									+ HttpStatus.BAD_REQUEST.getReasonPhrase());
						}
					}
					
					referenceIds.put(originalFullUrl, resource.getResourceType().toString() + "/" + resourceId);
				} else {
					response.setStatus(String.valueOf(HttpStatus.BAD_REQUEST.value()) + " "
							+ HttpStatus.BAD_REQUEST.getReasonPhrase());
				}

				entry.setRequest(null);
			}
		}
	}
	
	private void processPostComposition(IGenericClient client, List<BundleEntryComponent> entries) {
		for (BundleEntryComponent entry : entries) {
			BundleEntryResponseComponent response = entry.getResponse();
			if (response != null && !response.isEmpty()) {
				// We have already processed this.
				continue;
			}

			Resource resource = entry.getResource();
			if (resource instanceof Composition) {
				Composition composition = (Composition) resource;
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
				
				// check if we already have a composition for this patient.
				IdType compositionIdType = null;
				IdType subjectIdType = new IdType("Patient", patientId);
				Bundle resultBundle = client.search().forResource(Composition.class).where(Composition.SUBJECT.hasId(subjectIdType)).returnBundle(Bundle.class).execute();
				if (resultBundle != null && !resultBundle.isEmpty()) {
					BundleEntryComponent compositionEntry = resultBundle.getEntryFirstRep();
					if (compositionEntry != null && !compositionEntry.isEmpty()) {
						Composition existingComposition = (Composition) compositionEntry.getResource();
						compositionIdType = existingComposition.getIdElement();
					}
				}
				
				MethodOutcome outcome;
				if (compositionIdType == null) {
					outcome = client.create().resource(composition).prettyPrint().encodedJson().execute();
				} else {
					composition.setId(compositionIdType);
					outcome = client.update().resource(composition).prettyPrint().encodedJson().execute();
				}
				
				if (outcome.getCreated() != null && outcome.getCreated()) {
					String compositionId = outcome.getId().getIdPart();
					referenceIds.put(entry.getFullUrl(), "Composition/" + compositionId);

					response.setStatus(
							String.valueOf(HttpStatus.CREATED.value()) + " " + HttpStatus.CREATED.getReasonPhrase());
					response.setLocation(CompositionResourceProvider.getType() + "/" + compositionId);

					entry.setFullUrl(client.getServerBase() + "Composition/" + compositionId);
					if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
						entry.setResource((Resource) outcome.getResource());
					}
				} else if (outcome.getId() != null && !outcome.getId().isEmpty()) {
					String compositionId = outcome.getId().getIdPart();
					referenceIds.put(entry.getFullUrl(), "Composition/" + compositionId);

					response.setStatus(
							String.valueOf(HttpStatus.OK.value()) + " " + HttpStatus.OK.getReasonPhrase());
					entry.setFullUrl(client.getServerBase() + "Composition/" + compositionId);
					if (outcome.getResource() != null && !outcome.getResource().isEmpty()) {
						entry.setResource((Resource) outcome.getResource());
					}					
				} else {
					response.setStatus(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()) + " "
							+ HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
				}
				 

				entry.setRequest(null);
			}
		}
	}

//	private void createComposition(IGenericClient client, List<BundleEntryComponent> entries) {
//		// Check if we already have a composition for the same patient
//		Bundle responseBundle = client.search().forResource(Composition.class)
//				.where(Composition.SUBJECT.hasAnyOfIds(patientId))
//				.and(Composition.TYPE.exactly().systemAndCode("http://loinc.org", "64297-5")).returnBundle(Bundle.class)
//				.execute();
//
//		Composition composition;
//		boolean create = true;
//		if (responseBundle.getTotal() > 0) {
//			composition = (Composition) responseBundle.getEntryFirstRep().getResource();
//			create = false; // we should update
//		} else {
//			// Create a composition for this batch write.
//			composition = new Composition();
//			composition.setType(new CodeableConcept(new Coding("http://loinc.org", "64297-5", "Death certificate")));
//		}
//
//		// Walk through the entries again and add the entries to composition	
//		SectionComponent sectionComponent = new SectionComponent();
//		for (BundleEntryComponent entry : entries) {
//			Resource resource = entry.getResource();
//			if (resource instanceof Patient) {
//				composition.setSubject(new Reference(resource.getIdElement()));
//			} 
//			sectionComponent.addEntry(new Reference(resource));
//		}
//		
//		composition.getSection().add(sectionComponent);
//		MethodOutcome mo;
//		if (create) {
//			mo = client.create().resource(composition).encodedJson().prettyPrint().execute();
//		} else {
//			mo = client.update().resource(composition).encodedJson().prettyPrint().execute();
//		}
//		
//		if (mo.getId() == null) {
//			logger.debug("Failed to create a composition for the batch upload.");
//		}
//		
//	}

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
			processPostPatient(client, entries);
			processPostPractitioner(client, entries);
			processPostLocation(client, entries);
			processPostCondition(client, entries);
			processPost(client, entries);

			// Now, we should have composition left. 
			processPostComposition(client, entries);
			
//			processGet(theRequest.getRequestURL().toString(), entries);

//			createComposition(client, entries);

			theBundle.setType(BundleType.BATCHRESPONSE);

			break;
		case DOCUMENT:
			// We support two kinds of document here. 
			// One is VRDR, the other is Toxicology document
			// We post all in the entries (based on VRDR resources) to server.
			processPostPatient(client, entries);
			processPostPractitioner(client, entries);
			processPostLocation(client, entries);
			processPostCondition(client, entries);
			processPost(client, entries);

			// Now, we should have composition left. 
			processPostComposition(client, entries);
			
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
