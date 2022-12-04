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

import javax.annotation.PostConstruct;

import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.MessageHeader;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.DiagnosticReport.DiagnosticReportMediaComponent;
import org.hl7.fhir.r4.model.MessageHeader.MessageSourceComponent;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
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
import ca.uhn.fhir.rest.param.ReferenceAndListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.param.UriOrListParam;
import ca.uhn.fhir.rest.param.UriParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;
import edu.gatech.chai.fhironfhirbase.utilities.MdiProfileUtil;
import edu.gatech.chai.fhironfhirbase.utilities.OperationUtil;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;

@Service
@Scope("prototype")
public class DiagnosticReportResourceProvider extends BaseResourceProvider {

	/**
	 * Search parameter: <b>tox-lab-case-number</b>
	 * <p>
	 * Description: <b>A DiagnosticReport extension identfier for tox-lab-case-number</b><br>
   	 * Type: <b>token</b><br>
	 * Path: <b>DiagnosticReport.tox-lab-case-number</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name="tox-lab-case-number", path="DiagnosticReport.extension-tracking-numbers", description="Extension Trakcing Number for toxicology lab case", type="token" )
	public static final String SP_TOX_LAB_CASE_NUMBER = "tox-lab-case-number";
	/**
   	 * <b>Fluent Client</b> search parameter constant for <b>tox-lab-case-number</b>
	 * <p>
	 * Description: <b>A DiagnosticReport extension identfier for tox-lab-case-number</b><br>
   	 * Type: <b>token</b><br>
	 * Path: <b>DiagnosticReport.tox-lab-case-number</b><br>
	 * </p>
   	 */
	public static final ca.uhn.fhir.rest.gclient.TokenClientParam TOX_LAB_CASE_NUMBER = new ca.uhn.fhir.rest.gclient.TokenClientParam(SP_TOX_LAB_CASE_NUMBER);

	/**
	 * Search parameter: <b>mdi-case-number</b>
	 * <p>
	 * Description: <b>A DiagnosticReport extension identifier for mdi-case-number</b><br>
   	 * Type: <b>token</b><br>
	 * Path: <b>DiagnosticReport.mdi-case-number</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name="mdi-case-number", path="DiagnosticReport.extension-tracking-numbers", description="Extension Trakcing Number for MDI case", type="token" )
	public static final String SP_MDI_CASE_NUMBER = "mdi-case-number";
	/**
   	 * <b>Fluent Client</b> search parameter constant for <b>mdi-case-number</b>
   	 * <p>
	 * Description: <b>A DiagnosticReport extension identifier for mdi-case-number</b><br>
	 * Type: <b>token</b><br>
   	 * Path: <b>DiagnosticReport.mdi-case-number</b><br>
   	 * </p>
   	 */
  	public static final ca.uhn.fhir.rest.gclient.TokenClientParam MDI_CASE_NUMBER = new ca.uhn.fhir.rest.gclient.TokenClientParam(SP_MDI_CASE_NUMBER);


	public DiagnosticReportResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(DiagnosticReportResourceProvider.getType().toLowerCase());
		setMyResourceType(DiagnosticReportResourceProvider.getType());

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
			@OptionalParam(name = DiagnosticReportResourceProvider.SP_TOX_LAB_CASE_NUMBER) TokenOrListParam theToxLabCaseNumber,
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

		if (theToxLabCaseNumber != null) {
			String where = "";
			for (TokenParam toxLabCaseNumberToken : theToxLabCaseNumber.getValuesAsQueryTokens()) {
				String system = toxLabCaseNumberToken.getSystem();
				String value = toxLabCaseNumberToken.getValue();
				String whereItem;
				if (system == null || system.isEmpty()) {
					whereItem = "extensions @> '{\"url\": \"http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes\", \"code\":\"" + SP_TOX_LAB_CASE_NUMBER + "\"}]}, \"value\": \"" + value + "\"}}'::jsonb";
				} else {
					whereItem = "extensions @> '{\"url\": \"http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes\", \"code\":\"" + SP_TOX_LAB_CASE_NUMBER + "\"}]}, \"system\": \"" + system + "\", \"value\": \"" + value + "\"}}'::jsonb";
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
		
		// First find if we have a message header for this report.
		Bundle messageHeaders = client.search().forResource(MessageHeader.class)
			.where(MessageHeader.FOCUS.hasId(diagnosticReport.getIdElement())).returnBundle(Bundle.class).execute();
		if (messageHeaders != null && !messageHeaders.isEmpty() && messageHeaders.getTotal() > 0) {
			// We may have multiple messageheaders that focus on the same diagnosticreport. We choose the first one.
			messageHeader = (MessageHeader) messageHeaders.getEntryFirstRep().getResource();

			// Now, we find a bundle message that has this messageheader.
			Bundle respMessageBundle = client.search().forResource(Bundle.class)
				.where(Bundle.MESSAGE.hasId(messageHeader.getIdElement())).returnBundle(Bundle.class).execute();
			if (respMessageBundle != null) {
				// We have the Message bundle. Return this.
				return respMessageBundle;
			}
		} else {
			// Ceate MessageHeader
			UrlType endpoint = new UrlType(OperationUtil.myHostUrl());
			MessageSourceComponent source = new MessageSourceComponent(endpoint);
			messageHeader = new MessageHeader(MdiProfileUtil.TOXICOLOGY_LAB_RESULT, source);
			messageHeader.setId(new IdType(MessageHeaderResourceProvider.getType(), UUID.randomUUID().toString()));
			messageHeader.addFocus(new Reference(new IdType(diagnosticReport.getIdElement())));
		}

		// Create a message bundle and add to messageheader to the entry
		BundleEntryComponent bundleEntryComponent = new BundleEntryComponent().setFullUrl(messageHeader.getId()).setResource(messageHeader);
		retMessageBundle.addEntry(bundleEntryComponent); 

		// fill out other required fields
		retMessageBundle.setId(new IdType("Bundle", UUID.randomUUID().toString()));
		retMessageBundle.setIdentifier(OperationUtil.generateIdentifier(OperationUtil.RAVEN_TOX_SYSTEM));
		retMessageBundle.setType(BundleType.MESSAGE);

		// Add diagnosticreport to Bundle.entry
		bundleEntryComponent = new BundleEntryComponent().setFullUrl(diagnosticReport.getId()).setResource(diagnosticReport);
		retMessageBundle.addEntry(bundleEntryComponent);

		// Add Patient.
		Reference reference = diagnosticReport.getSubject();
		bundleEntryComponent = makeEntryResourceToEntry(client, reference);
		if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
			retMessageBundle.addEntry(bundleEntryComponent);
		}

		// Add Performer(s)
		List<Reference> references = diagnosticReport.getPerformer();
		for (Reference ref : references) {
			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add specimen
		references = diagnosticReport.getSpecimen();
		for (Reference ref : references) {
			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add result
		references = diagnosticReport.getResult();
		for (Reference ref : references) {
			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add basedOn (if available - this is an optional)
		references = diagnosticReport.getBasedOn();
		for (Reference ref : references) {
			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add encounter
		reference = diagnosticReport.getEncounter();
		bundleEntryComponent = makeEntryResourceToEntry(client, reference);
		if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
			retMessageBundle.addEntry(bundleEntryComponent);
		}

		// Add resultsInterpreter
		references = diagnosticReport.getResultsInterpreter();
		for (Reference ref : references) {
			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add imagingStudy
		references = diagnosticReport.getImagingStudy();
		for (Reference ref : references) {
			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}

		// Add media.link
		List<DiagnosticReportMediaComponent> medias = diagnosticReport.getMedia();
		for (DiagnosticReportMediaComponent media : medias) {
			reference = media.getLink();
			bundleEntryComponent = makeEntryResourceToEntry(client, reference);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}
		 for (Reference ref : references) {
			bundleEntryComponent = makeEntryResourceToEntry(client, ref);
			if (bundleEntryComponent != null && !bundleEntryComponent.isEmpty()) {
				retMessageBundle.addEntry(bundleEntryComponent);
			}
		}
		
		return retMessageBundle;
	}

	@Operation(name = "$toxicology-message", idempotent = true, bundleType = BundleTypeEnum.SEARCHSET)
	public Bundle generateToxicologyMessageOperation(RequestDetails theRequestDetails, 
			@IdParam(optional=true) IdType theDiagnosticReportId,
			@OperationParam(name = "id") UriOrListParam theIds, 
			@OperationParam(name = DiagnosticReport.SP_PATIENT) ParametersParameterComponent thePatient,
			@OperationParam(name = DiagnosticReportResourceProvider.SP_MDI_CASE_NUMBER) StringOrListParam theMdiCaseNumber,
			@OperationParam(name = DiagnosticReportResourceProvider.SP_TOX_LAB_CASE_NUMBER) StringOrListParam theToxLabCaseNumber) {
				
		String myFhirServerBase = theRequestDetails.getFhirServerBase();
		IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
		OperationUtil.setupClientForAuth(client);
		
		int totalSize = 0;

		Bundle retMessageBundle = new Bundle();

		OperationOutcome outcome = new OperationOutcome();
		if (theDiagnosticReportId != null) {
			// this is read by the id. Get the diagnostic report and construct message bundle.
			DiagnosticReport diagnosticReport = (DiagnosticReport) readDiagnosticReport(theDiagnosticReportId);
			if (diagnosticReport == null) {
				ThrowFHIRExceptions.unprocessableEntityException("DiagnosticReport.id, " + theDiagnosticReportId.asStringValue() + " does not exist");
			}

			return constructMessageBundleFromDiagnosticReport(client, diagnosticReport);
		}

		if (theIds != null) {
			// We search on IDs
			retMessageBundle.setType(BundleType.SEARCHSET);
			for (UriParam theId : theIds.getValuesAsQueryTokens()) {
				DiagnosticReport diagnosticReport = (DiagnosticReport) readDiagnosticReport(theDiagnosticReportId);
				if (diagnosticReport != null) {
					Bundle messageBundle = constructMessageBundleFromDiagnosticReport(client, diagnosticReport);
					BundleEntryComponent bundleEntryComponent = new BundleEntryComponent().setFullUrl(messageBundle.getIdElement().asStringValue()).setResource(messageBundle);
					retMessageBundle.addEntry(bundleEntryComponent);
				
					totalSize++;
				}
			}

			retMessageBundle.setTotal(totalSize);

			return retMessageBundle;
		}

		boolean shouldQuery = false;

		IQuery<IBaseBundle> query = client.search().forResource(DiagnosticReport.class);

		if (addTokenToQuery(query, MDI_CASE_NUMBER, theMdiCaseNumber) == true) {
			shouldQuery = true;
		}

 		if (addTokenToQuery(query, TOX_LAB_CASE_NUMBER, theToxLabCaseNumber) == true) {
			shouldQuery = true;
		}

		Bundle diagnosticReportBundle = null;
		if (shouldQuery) {
			diagnosticReportBundle = query.returnBundle(Bundle.class).execute();
		}

		retMessageBundle.setType(BundleType.SEARCHSET);
		if (diagnosticReportBundle != null && !diagnosticReportBundle.isEmpty()) {
			List<BundleEntryComponent> entries = diagnosticReportBundle.getEntry();
			for (BundleEntryComponent entry : entries) {
				DiagnosticReport diagnosticReport = (DiagnosticReport) entry.getResource();
				if (diagnosticReport != null) {
					Bundle messageBundle = constructMessageBundleFromDiagnosticReport(client, diagnosticReport);
					BundleEntryComponent bundleEntryComponent = new BundleEntryComponent().setFullUrl(messageBundle.getIdElement().asStringValue()).setResource(messageBundle);
					retMessageBundle.addEntry(bundleEntryComponent);
	
					totalSize++;
				}
			}
	
		}

		retMessageBundle.setTotal(totalSize);

		return retMessageBundle;
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

	/* TODO: 
	 * Add more validation code here.
	 */
	private void validateResource(DiagnosticReport theDiagnosticReport) {
		
	}



}
