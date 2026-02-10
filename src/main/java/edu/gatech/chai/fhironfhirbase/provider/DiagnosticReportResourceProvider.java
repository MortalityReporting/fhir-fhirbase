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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import jakarta.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.MessageHeader;
import org.hl7.fhir.r4.model.PractitionerRole;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UrlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.DiagnosticReport.DiagnosticReportMediaComponent;
import org.hl7.fhir.r4.model.MessageHeader.MessageSourceComponent;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Patient;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.model.api.annotation.SearchParamDefinition;
import ca.uhn.fhir.model.valueset.BundleTypeEnum;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.IncludeParam;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Sort;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.ParameterUtil;
import ca.uhn.fhir.rest.param.ReferenceAndListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.param.UriOrListParam;
import ca.uhn.fhir.rest.param.UriParam;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.provider.CompositionResourceProvider.MyDocumentBundle;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;
import edu.gatech.chai.fhironfhirbase.utilities.MdiProfileUtil;
import edu.gatech.chai.fhironfhirbase.utilities.OperationUtil;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;

@Service
@Scope("prototype")
public class DiagnosticReportResourceProvider extends BaseResourceProvider {
	private static final Logger logger = LoggerFactory.getLogger(DiagnosticReportResourceProvider.class);

	/**
	 * Search parameter: <b>tox-lab-case-number</b>
	 * <p>
	 * Description: <b>A DiagnosticReport extension identfier for tox-lab-case-number</b><br>
   	 * Type: <b>token</b><br>
	 * Path: <b>DiagnosticReport.tox-lab-case-number</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name="tracking-number", path="DiagnosticReport.extension-tracking-numbers", description="Extension Trakcing Number for toxicology lab case", type="token" )
	public static final String SP_TRACKING_NUMBER = "tracking-number";
	/**
   	 * <b>Fluent Client</b> search parameter constant for <b>tox-lab-case-number</b>
	 * <p>
	 * Description: <b>A DiagnosticReport extension identfier for tox-lab-case-number</b><br>
   	 * Type: <b>token</b><br>
	 * Path: <b>DiagnosticReport.tox-lab-case-number</b><br>
	 * </p>
   	 */
	public static final ca.uhn.fhir.rest.gclient.TokenClientParam TRACKING_NUMBER = new ca.uhn.fhir.rest.gclient.TokenClientParam(SP_TRACKING_NUMBER);

	// /**
	//  * Search parameter: <b>mdi-case-number</b>
	//  * <p>
	//  * Description: <b>A DiagnosticReport extension identifier for mdi-case-number</b><br>
   	//  * Type: <b>token</b><br>
	//  * Path: <b>DiagnosticReport.mdi-case-number</b><br>
	//  * </p>
	//  */
	// @SearchParamDefinition(name="mdi-case-number", path="DiagnosticReport.extension-tracking-numbers", description="Extension Trakcing Number for MDI case", type="token" )
	// public static final String SP_MDI_CASE_NUMBER = "mdi-case-number";
	// /**
   	//  * <b>Fluent Client</b> search parameter constant for <b>mdi-case-number</b>
   	//  * <p>
	//  * Description: <b>A DiagnosticReport extension identifier for mdi-case-number</b><br>
	//  * Type: <b>token</b><br>
   	//  * Path: <b>DiagnosticReport.mdi-case-number</b><br>
   	//  * </p>
   	//  */
  	// public static final ca.uhn.fhir.rest.gclient.TokenClientParam MDI_CASE_NUMBER = new ca.uhn.fhir.rest.gclient.TokenClientParam(SP_MDI_CASE_NUMBER);


	public DiagnosticReportResourceProvider(FhirContext ctx) {
		super(ctx);

		setTableName(DiagnosticReportResourceProvider.getType().toLowerCase());
		setMyResourceType(DiagnosticReportResourceProvider.getType());
	}

	@PostConstruct
	private void postConstruct() {
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	public static String getType() {
		return "DiagnosticReport";
	}

	@Override
	public Class<DiagnosticReport> getResourceType() {
		return DiagnosticReport.class;
	}

	@Create()
	public MethodOutcome createDiagnosticReport(@ResourceParam DiagnosticReport theDiagnosticReport) {
		validateResource(theDiagnosticReport);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdDiagnosticReport = getFhirbaseMapping().create(theDiagnosticReport, getResourceType());
			retVal.setId(createdDiagnosticReport.getIdElement());
			retVal.setResource(createdDiagnosticReport);
			retVal.setCreated(true);
		} catch (SQLException e) {
			retVal.setCreated(false);
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

		return retVal;
	}

	@Delete()
	public void deleteDiagnosticReport(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	@Read()
	public IBaseResource readDiagnosticReport(@IdParam IdType theId) {
		IBaseResource retVal = null;
		
		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Update()
	public MethodOutcome updateDiagnosticReport(@IdParam IdType theId,
			@ResourceParam DiagnosticReport theDiagnosticReport) {
		validateResource(theDiagnosticReport);

		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedDiagnosticReport = getFhirbaseMapping().update(theDiagnosticReport, getResourceType());
			retVal.setId(updatedDiagnosticReport.getIdElement());
			retVal.setResource(updatedDiagnosticReport);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Search()
	public IBundleProvider findDiagnosticReportByIds(
			@RequiredParam(name = DiagnosticReport.SP_RES_ID) TokenOrListParam theDiagnosticReportIds,

			@IncludeParam(allow = { "DiagnosticReport:patient", "DiagnosticReport:subject",
					"DiagnosticReport:encounter" }) final Set<Include> theIncludes,

			@IncludeParam(reverse = true) final Set<Include> theReverseIncludes) {

		if (theDiagnosticReportIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theDiagnosticReport : theDiagnosticReportIds.getValuesAsQueryTokens()) {
			whereStatement += "diag.id = '" + theDiagnosticReport.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM documentreference diag " + whereStatement;
		String query = "SELECT * FROM documentreference diag " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, theReverseIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findDiagnosticReportsByParams(
			@OptionalParam(name = DiagnosticReport.SP_PATIENT, chainWhitelist = { "",
					USCorePatient.SP_NAME }) ReferenceAndListParam thePatients,
			@OptionalParam(name = DiagnosticReport.SP_SUBJECT, chainWhitelist = { "",
					USCorePatient.SP_NAME }) ReferenceAndListParam theSubjects,
			@OptionalParam(name = DiagnosticReportResourceProvider.SP_TRACKING_NUMBER) TokenOrListParam theTrackingNumber,
			@OptionalParam(name = DiagnosticReport.SP_IDENTIFIER) TokenParam theDocumentReferenceIdentifier,
			@OptionalParam(name = DiagnosticReport.SP_ENCOUNTER) ReferenceParam theEncounter,
			@Sort SortSpec theSort,
			@IncludeParam(allow = { "DiagnosticReport:patient", "DiagnosticReport:subject",
					"DiagnosticReport:encounter" }) final Set<Include> theIncludes,

			@IncludeParam(reverse = true) final Set<Include> theReverseIncludes) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = getTableName() + " diag";

		if (theSubjects != null || thePatients != null) {
			fromStatement += " join patient p on diag.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";

			String updatedFromStatement = constructFromWherePatients (fromStatement, whereParameters, theSubjects);
			if (updatedFromStatement.isEmpty()) {
				// This means that we have unsupported resource. Since this is to search, we should discard all and
				// return null.
				return null;
			}
			fromStatement = updatedFromStatement;

			updatedFromStatement = constructFromWherePatients(fromStatement, whereParameters, thePatients);
			if (updatedFromStatement.isEmpty()) {
				// This means that we have unsupported resource. Since this is to search, we should discard all and
				// return null.
				return null;
			}
			fromStatement = updatedFromStatement;
			
			returnAll = false;			
		}

		if (theEncounter != null) {
			whereParameters.add("diag.resource->'context'->>'encounter' like '%" + theEncounter.getValue() + "%'");
			returnAll = false;
		}

		if (theDocumentReferenceIdentifier != null) {
			String system = theDocumentReferenceIdentifier.getSystem();
			String value = theDocumentReferenceIdentifier.getValue();

			if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
				whereParameters.add("diag.resource->'identifier' @> '[{\"value\": \"" + value + "\",\"system\": \""
						+ system + "\"}]'::jsonb");
			} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
				whereParameters.add("diag.resource->'identifier' @> '[{\"system\": \"" + system + "\"}]'::jsonb");
			} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
				whereParameters.add("diag.resource->'identifier' @> '[{\"value\": \"" + value + "\"}]'::jsonb");
			}
			returnAll = false;
		}

		if (theTrackingNumber != null) {
			fromStatement = constructFromStatementPath(fromStatement, "extensions", "diag.resource->'extension'");

			String where = "";
			for (TokenParam toxLabCaseNumberToken : theTrackingNumber.getValuesAsQueryTokens()) {
				String system = toxLabCaseNumberToken.getSystem();
				String value = toxLabCaseNumberToken.getValue();
				String whereItem;
				if (system == null || system.isEmpty()) {
					whereItem = "extensions @> '{\"url\": \"" + ExtensionUtil.extTrackingNumberUrl + "\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"" + ExtensionUtil.extTrackingNumberTypeSystem + "\"}]}, \"value\": \"" + value + "\"}}'::jsonb";
				} else {
					whereItem = "extensions @> '{\"url\": \"" + ExtensionUtil.extTrackingNumberUrl + "\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"" + ExtensionUtil.extTrackingNumberTypeSystem + "\"}]}, \"system\": \"" + system + "\", \"value\": \"" + value + "\"}}'::jsonb";
				}
				if (where.isEmpty()) {
					where = whereItem;
				} else {
					where += " or " + whereItem;
				}
			}

			whereParameters.add(where);
			returnAll = false;
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll && (whereStatement == null || whereStatement.isEmpty())) { 
			return null;
		}

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, theIncludes, theReverseIncludes);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	private BundleEntryComponent makeEntryResourceToEntry (IGenericClient client, Reference reference) {
		if (reference == null || reference.isEmpty()) {
			return null;
		}

		// check if this is empty with data-absent-reason
		if (reference.getReference() == null) {
			return null;
		}

		Resource resource = (Resource) client.read()
		.resource(reference.getReferenceElement().getResourceType())
		.withId(reference.getReferenceElement().getIdPart()).encodedJson()
		.execute();

		BundleEntryComponent bundleEntryComponent = new BundleEntryComponent();
		bundleEntryComponent.setFullUrl(reference.getReference());
		bundleEntryComponent.setResource(resource);
	
		return bundleEntryComponent;
	}

	private Bundle constructMessageBundleFromDiagnosticReport(IGenericClient client, DiagnosticReport diagnosticReport) {
		Bundle retMessageBundle = new Bundle();
		MessageHeader messageHeader;
		Bundle originalMessageBundle = null;
		
		// First find if we have a message header for this report.
		String focusId = DiagnosticReportResourceProvider.getType() + "/" + diagnosticReport.getIdElement().getIdPart();
		Bundle messageHeaders = client.search().forResource(MessageHeader.class)
			.where(MessageHeader.FOCUS.hasId(focusId)).returnBundle(Bundle.class).execute();
		if (messageHeaders != null && !messageHeaders.isEmpty() && messageHeaders.getTotal() > 0) {
			// We may have multiple messageheaders that focus on the same diagnosticreport. We choose the first one.
			messageHeader = (MessageHeader) messageHeaders.getEntryFirstRep().getResource();

			// Now, we find a bundle message that has this messageheader.
			String mhId = MessageHeaderResourceProvider.getType() + "/" + messageHeader.getIdElement().getIdPart();
			Bundle respMessageBundle = client.search().forResource(Bundle.class)
				.where(Bundle.MESSAGE.hasId(mhId)).returnBundle(Bundle.class).execute();
			if (respMessageBundle != null) {
				// We have the search bundle that contains message bundle. Since we are searching
				// one message bundle, just send the first one. 
				if (respMessageBundle.getTotal() > 0) {
					originalMessageBundle = (Bundle) respMessageBundle.getEntryFirstRep().getResource();
				}
			}
		} else {
			// Ceate MessageHeader
			UrlType endpoint = new UrlType(OperationUtil.myHostUrl());
			MessageSourceComponent source = new MessageSourceComponent(endpoint);
			messageHeader = new MessageHeader(MdiProfileUtil.TOXICOLOGY_LAB_RESULT, source);
			messageHeader.setId(new IdType(MessageHeaderResourceProvider.getType(), UUID.randomUUID().toString()));
			messageHeader.addFocus(new Reference(new IdType(diagnosticReport.getIdElement())));
		}

		// Add profile to Message Bundle
		retMessageBundle.getMeta().addProfile("http://hl7.org/fhir/us/mdi/StructureDefinition/Bundle-message-tox-to-mdi");

		// Create a message bundle and add to messageheader to the entry
		String messageHeaderLocalUrl = MessageHeaderResourceProvider.getType() + "/" + messageHeader.getIdElement().getIdPart();
		BundleEntryComponent bundleEntryComponent = new BundleEntryComponent().setFullUrl(messageHeaderLocalUrl).setResource(messageHeader);
		retMessageBundle.addEntry(bundleEntryComponent); 

		// fill out other required fields
		retMessageBundle.setType(BundleType.MESSAGE);

		// Add diagnosticreport to Bundle.entry
		String diagnosticReportLocalUrl = DiagnosticReportResourceProvider.getType() + "/" + diagnosticReport.getIdElement().getIdPart();
		bundleEntryComponent = new BundleEntryComponent().setFullUrl(diagnosticReportLocalUrl).setResource(diagnosticReport);
		retMessageBundle.addEntry(bundleEntryComponent);

		// list of references added.
		List<Reference> addedReferences = new ArrayList<Reference>();

		// Add Patient.
		Reference reference = diagnosticReport.getSubject();
		bundleEntryComponent = makeEntryResourceToEntry(client, reference);
		if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
			retMessageBundle.addEntry(bundleEntryComponent);
			addedReferences.add(reference);
		}

		// Add Performer(s)
		List<Reference> references = diagnosticReport.getPerformer();
		for (Reference ref : references) {
			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
				addedReferences.add(ref);

				if (PractitionerRoleResourceProvider.getType().equals(ref.getReferenceElement().getResourceType())) {
					// get the resource and its practitioner, organization, locations, and endpoints
					PractitionerRole practitionerRole = (PractitionerRole) bundleEntryComponent.getResource();
					
					Reference practitionerRef = practitionerRole.getPractitioner();
					if (!addedReferences.contains(practitionerRef)) {
						bundleEntryComponent = makeEntryResourceToEntry(client, practitionerRef);
						if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
							retMessageBundle.addEntry(bundleEntryComponent);
							addedReferences.add(practitionerRef);
						}
					}

					Reference organizationRef = practitionerRole.getOrganization();
					if (!addedReferences.contains(organizationRef)) {
						bundleEntryComponent = makeEntryResourceToEntry(client, organizationRef);
						if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
							retMessageBundle.addEntry(bundleEntryComponent);
							addedReferences.add(organizationRef);
						}
					}

					List<Reference> locationRefs = practitionerRole.getLocation();
					for (Reference locationRef : locationRefs) {
						if (!addedReferences.contains(locationRef)) {
							bundleEntryComponent = makeEntryResourceToEntry(client, locationRef);
							if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
								retMessageBundle.addEntry(bundleEntryComponent);
								addedReferences.add(locationRef);
							}
						}
					}

					List<Reference> endpointRefs = practitionerRole.getEndpoint();
					for (Reference endpointRef : endpointRefs) {
						if (!addedReferences.contains(endpointRef)) {
							bundleEntryComponent = makeEntryResourceToEntry(client, endpointRef);
							if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
								retMessageBundle.addEntry(bundleEntryComponent);
								addedReferences.add(endpointRef);
							}
						}
					}
				}	
			}
		}

		// Add specimen
		references = diagnosticReport.getSpecimen();
		for (Reference ref : references) {
			if (addedReferences.contains(ref)) continue;

			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add result
		references = diagnosticReport.getResult();
		for (Reference ref : references) {
			if (addedReferences.contains(ref)) continue;

			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add basedOn (if available - this is an optional)
		references = diagnosticReport.getBasedOn();
		for (Reference ref : references) {
			if (addedReferences.contains(ref)) continue;

			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add encounter
		reference = diagnosticReport.getEncounter();
		if (!addedReferences.contains(reference)) {
			bundleEntryComponent = makeEntryResourceToEntry(client, reference);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add resultsInterpreter
		references = diagnosticReport.getResultsInterpreter();
		for (Reference ref : references) {
			if (addedReferences.contains(ref)) continue;

			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add imagingStudy
		references = diagnosticReport.getImagingStudy();
		for (Reference ref : references) {
			if (addedReferences.contains(ref)) continue;

			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add media.link
		List<DiagnosticReportMediaComponent> medias = diagnosticReport.getMedia();
		for (DiagnosticReportMediaComponent media : medias) {
			reference = media.getLink();
			if (addedReferences.contains(reference)) continue;

			bundleEntryComponent = makeEntryResourceToEntry(client, reference);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}
		// for (Reference ref : references) {
		// 	bundleEntryComponent = makeEntryResourceToEntry(client, ref);
		// 	if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
		// 		retMessageBundle.addEntry(bundleEntryComponent);
		// 	}
		// }

		if (originalMessageBundle != null) {
			retMessageBundle.setId(originalMessageBundle.getIdElement());
			retMessageBundle.setIdentifier(originalMessageBundle.getIdentifier());
			client.update().resource(retMessageBundle).prettyPrint().encodedJson().execute();
		} else {
			retMessageBundle.setId(new IdType("Bundle", UUID.randomUUID().toString()));
			retMessageBundle.setIdentifier(OperationUtil.generateIdentifier(OperationUtil.RAVEN_SYSTEM));	
			client.create().resource(retMessageBundle).prettyPrint().encodedJson().execute();
		}

		return retMessageBundle;
	}

	@Operation(name = "$toxicology-message", idempotent = true)
	public Bundle generateToxicologyMessageOperation(RequestDetails theRequestDetails, @IdParam IdType theId) {
		if (theId == null) {
			ThrowFHIRExceptions.unprocessableEntityException("DiagnosticReport.id is required for this operation.");
		}

		String myFhirServerBase = System.getenv("INTERNAL_FHIR_REQUEST_URL");
		if (myFhirServerBase == null || myFhirServerBase.isEmpty()) {
			myFhirServerBase = "http://localhost:8080/fhir";
		}
		IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
		OperationUtil.setupClientForAuth(client);

		DiagnosticReport diagnosticReport = (DiagnosticReport) readDiagnosticReport(theId);
			if (diagnosticReport == null) {
				ThrowFHIRExceptions.unprocessableEntityException("DiagnosticReport.id, " + theId.asStringValue() + " does not exist");
			}

		return  constructMessageBundleFromDiagnosticReport(client, diagnosticReport);
	}

	@Operation(name = "$toxicology-message", idempotent = true, bundleType = BundleTypeEnum.SEARCHSET)
	public IBundleProvider generateToxicologyMessageOperation(RequestDetails theRequestDetails, 
			@OperationParam(name = DiagnosticReport.SP_PATIENT) List<ParametersParameterComponent> thePatients,
			@OperationParam(name = DiagnosticReportResourceProvider.SP_TRACKING_NUMBER) StringOrListParam theTrackingNumber) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = getTableName() + " diag";

		// Set up join statements.
		if (thePatients != null) {
			// join patient and composition subject tables
			fromStatement += " join patient p on diag.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";
		}

		if (thePatients != null) {
			for (ParametersParameterComponent thePatient : thePatients) {
				for (ParametersParameterComponent patientParam : thePatient.getPart()) {
					String wheres = null;
					if (Patient.SP_FAMILY.equals(patientParam.getName())) {
						// we have family value. Add name field to from statement
						fromStatement = constructFromStatementPatientChain(fromStatement, Patient.SP_FAMILY);
						StringType theFamilies = (StringType) patientParam.getValue();
						if (theFamilies != null && !theFamilies.isEmpty()) {
							String[] familyStrings = theFamilies.asStringValue().split(",");
							for (String family : familyStrings) {
								if (wheres == null) {
									wheres = "lower(names::text)::jsonb @> lower('{\"family\":\"" + family
											+ "\"}')::jsonb";
								} else {
									wheres += "or lower(names::text)::jsonb @> lower('{\"family\":\"" + family
											+ "\"}')::jsonb";
								}
							}
						}
					} else if (Patient.SP_GIVEN.equals(patientParam.getName())) {
						// we have family value. Add name field to from statement
						fromStatement = constructFromStatementPatientChain(fromStatement, Patient.SP_GIVEN);
						StringType theGivens = (StringType) patientParam.getValue();
						if (theGivens != null && !theGivens.isEmpty()) {
							String[] givenStrings = theGivens.asStringValue().split(",");
							for (String given : givenStrings) {
								if (wheres == null) {
									wheres = "lower(names::text)::jsonb @> lower('{\"given\":[\"" + given
											+ "\"]}')::jsonb";
								} else {
									wheres += " or lower(names::text)::jsonb @> lower('{\"given\":[\"" + given
											+ "\"]}')::jsonb";
								}
							}
						}
					} else if (Patient.SP_GENDER.equals(patientParam.getName())) {
						// we have gender value. Add name field to from statement
						fromStatement = constructFromStatementPatientChain(fromStatement, Patient.SP_GENDER);
						StringType theGenders = (StringType) patientParam.getValue();
						if (theGenders != null && !theGenders.isEmpty()) {
							String[] genderStrings = theGenders.asStringValue().split(",");
							for (String gender : genderStrings) {
								if (wheres == null) {
									wheres = "p.resource->>'gender' = " + "'" + gender + "'";
								} else {
									wheres += " or p.resource->>'gender' = " + "'" + gender + "'";
								}
							}
						}
					} else if (Patient.SP_BIRTHDATE.equals(patientParam.getName())) {
						StringType birthDate = (StringType) patientParam.getValue();
						DateParam date = getDateParam(birthDate.asStringValue());
						wheres = constructDateWhereParameter(date, "p", "birthDate");
					}

					if (wheres != null) {
						whereParameters.add(wheres);
					}
				}
			}
		}

		if (theTrackingNumber != null) {
			fromStatement = constructFromStatementPath(fromStatement, "extensions", "comp.resource->'extension'");

			String wheres = null;
			for (StringParam tokenParam : theTrackingNumber.getValuesAsQueryTokens()) {
				String token = tokenParam.getValue();
				int barIndex = ParameterUtil.nonEscapedIndexOf(token, '|');
				String system = null;
				String value = null;
				if (barIndex != -1) {
					system = token.substring(0, barIndex);
					value = ParameterUtil.unescape(token.substring(barIndex + 1));
				} else {
					value = ParameterUtil.unescape(token);
				}

				String whereItem;
				if (system == null || system.isBlank()) {
					whereItem = "extensions @> '{\"url\": \"" + ExtensionUtil.extTrackingNumberUrl
							+ "\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \""
							+ ExtensionUtil.extTrackingNumberTypeSystem + "\"}]}, \"value\": \"" + value
							+ "\"}}'::jsonb";
				} else if (value == null || value.isBlank()) {
					whereItem = "extensions @> '{\"url\": \"" + ExtensionUtil.extTrackingNumberUrl
							+ "\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \""
							+ ExtensionUtil.extTrackingNumberTypeSystem + "\"}]}, \"system\": \"" + system
							+ "\"}}'::jsonb";
				} else {
					whereItem = "extensions @> '{\"url\": \"" + ExtensionUtil.extTrackingNumberUrl
							+ "\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \""
							+ ExtensionUtil.extTrackingNumberTypeSystem + "\"}]}, \"system\": \"" + system
							+ "\", \"value\": \"" + value + "\"}}'::jsonb";
				}

				if (wheres == null) {
					wheres = whereItem;
				} else {
					wheres += " or " + whereItem;
				}
			}

			whereParameters.add(wheres);
		}

		String whereStatement = constructWhereStatement(whereParameters, null);

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT diag.resource as resource FROM " + fromStatement + whereStatement;

		logger.debug("query count:" + queryCount + "\nquery:" + query);

		MyMessageBundle myMessageBundleProvider = new MyMessageBundle(query, theRequestDetails, null, null);
		myMessageBundleProvider.setTotalSize(getTotalSize(queryCount));
		myMessageBundleProvider.setPreferredPageSize(preferredPageSize);

		return myMessageBundleProvider;
	}


	class MyBundleProvider extends FhirbaseBundleProvider {
		Set<Include> theIncludes;
		Set<Include> theReverseIncludes;

		public MyBundleProvider(String query, Set<Include> theIncludes, Set<Include> theReverseIncludes) {
			super(query);
			setPreferredPageSize(preferredPageSize);
			this.theIncludes = theIncludes;
			this.theReverseIncludes = theReverseIncludes;
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();
			
			// _Include
			List<String> includes = new ArrayList<String>();

			if (theIncludes.contains(new Include("DiagnosticReport:encounter"))) {
				includes.add("DiagnosticReport:encounter");
			}

			if (theIncludes.contains(new Include("DiagnosticReport:patient"))) {
				includes.add("DiagnosticReport:patient");
			}

			if (theIncludes.contains(new Include("DiagnosticReport:subject"))) {
				includes.add("DiagnosticReport:subject");
			}

			String myQuery = query;			
			if (toIndex - fromIndex > 0) {
				myQuery += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			try {
				retVal.addAll(getFhirbaseMapping().search(myQuery, getResourceType()));
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			return retVal;
		}
	}

	class MyMessageBundle extends FhirbaseBundleProvider {
		RequestDetails theRequestDetails;

		public MyMessageBundle(String query, RequestDetails theRequestDetails, Set<Include> theIncludes,
				Set<Include> theReverseIncludes) {
			super(query);
			setPreferredPageSize(preferredPageSize);
			this.theRequestDetails = theRequestDetails;
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			List<IBaseResource> messageBundles = new ArrayList<IBaseResource>();
			List<IBaseResource> retResources = new ArrayList<IBaseResource>();

			String myQuery = query;
			if (toIndex - fromIndex > 0) {
				myQuery += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			logger.debug("Generate diagnostic report message from database: " + myQuery);
			try {
				retResources = getFhirbaseMapping().search(myQuery, getResourceType());
			} catch (SQLException e) {
				e.printStackTrace();
			}

			// Make the diagnosticReport to Message Bundle.
			String myFhirServerBase = System.getenv("INTERNAL_FHIR_REQUEST_URL");
			if (myFhirServerBase == null || myFhirServerBase.isEmpty()) {
				myFhirServerBase = "http://localhost:8080/fhir";
			}
			IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
			OperationUtil.setupClientForAuth(client);

			for (IBaseResource diagnosticReport : retResources) {
				DiagnosticReport diagnosticReport_ = (DiagnosticReport) diagnosticReport;
				Bundle retBundle = constructMessageBundleFromDiagnosticReport(client, diagnosticReport_);

				messageBundles.add(retBundle);
			}

			return messageBundles;
		}
	}

	/* TODO: 
	 * Add more validation code here.
	 */
	private void validateResource(DiagnosticReport theDiagnosticReport) {
		
	}



}
