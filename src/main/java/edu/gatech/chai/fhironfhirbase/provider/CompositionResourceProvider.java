package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleLinkComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.ListResource;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RelatedPerson;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Practitioner;
import org.hl7.fhir.r4.model.PractitionerRole;
import org.hl7.fhir.r4.model.Procedure;
import org.hl7.fhir.r4.model.Procedure.ProcedurePerformerComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.model.valueset.BundleTypeEnum;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
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
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.param.DateOrListParam;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.DateRangeParam;
import ca.uhn.fhir.rest.param.ParamPrefixEnum;
import ca.uhn.fhir.rest.param.ReferenceAndListParam;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenAndListParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.param.UriOrListParam;
import ca.uhn.fhir.rest.param.UriParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class CompositionResourceProvider extends BaseResourceProvider {
	private static final Logger logger = LoggerFactory.getLogger(CompositionResourceProvider.class);
	public static final String NQ_EVENT_DETAIL = "event-detail";
	public static final String SP_DEATH_LOCATION = "vrdr-death-location.district";
	public static final String SP_DEATH_DATE = "vrdr-death-date.value-date";

	public CompositionResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(CompositionResourceProvider.getType().toLowerCase());
		setMyResourceType(CompositionResourceProvider.getType());
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	public static String getType() {
		return "Composition";
	}

	@Override
	public Class<Composition> getResourceType() {
		return Composition.class;
	}

	@Create()
	public MethodOutcome createComposition(@ResourceParam Composition theComposition) {
		validateResource(theComposition);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource createdObservation = getFhirbaseMapping().create(theComposition, getResourceType());
			retVal.setId(createdObservation.getIdElement());
			retVal.setResource(createdObservation);
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
	public void deleteComposition(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	@Search()
	public IBundleProvider findCompositionsById(
			@RequiredParam(name = Composition.SP_RES_ID) TokenOrListParam theCompositionIds, @Sort SortSpec theSort) {

		if (theCompositionIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theComposition : theCompositionIds.getValuesAsQueryTokens()) {
			whereStatement += "comp.id = '" + theComposition.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM " + getTableName() + " comp " + whereStatement;
		String query = "SELECT * FROM " + getTableName() + " comp " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, null, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findCompositionsByParams(
			@OptionalParam(name = Composition.SP_TYPE) TokenOrListParam theOrTypes,
			@OptionalParam(name = Composition.SP_DATE) DateParam theDate,
			@OptionalParam(name = CompositionResourceProvider.SP_DEATH_LOCATION) StringOrListParam theDeathLocations,
			@OptionalParam(name = CompositionResourceProvider.SP_DEATH_DATE) DateRangeParam theDeathDate,
			@OptionalParam(name = Composition.SP_PATIENT, chainWhitelist = { "", 
					USCorePatient.SP_ADDRESS_CITY,
					USCorePatient.SP_ADDRESS_COUNTRY,
					USCorePatient.SP_ADDRESS_POSTALCODE,
					USCorePatient.SP_ADDRESS_STATE,
					USCorePatient.SP_ADDRESS_USE,
					USCorePatient.SP_BIRTHDATE,
					USCorePatient.SP_DEATH_DATE,
					USCorePatient.SP_EMAIL,
					USCorePatient.SP_FAMILY,
					USCorePatient.SP_GENDER,
					USCorePatient.SP_GIVEN,
					USCorePatient.SP_NAME,
					USCorePatient.SP_PHONE,
					USCorePatient.SP_TELECOM,
					USCorePatient.SP_IDENTIFIER }) ReferenceAndListParam thePatients,
			@OptionalParam(name = Composition.SP_SUBJECT, chainWhitelist = { "", 
					USCorePatient.SP_ADDRESS_CITY,
					USCorePatient.SP_ADDRESS_COUNTRY,
					USCorePatient.SP_ADDRESS_POSTALCODE,
					USCorePatient.SP_ADDRESS_STATE,
					USCorePatient.SP_ADDRESS_USE,
					USCorePatient.SP_BIRTHDATE,
					USCorePatient.SP_DEATH_DATE,
					USCorePatient.SP_EMAIL,
					USCorePatient.SP_FAMILY,
					USCorePatient.SP_GENDER,
					USCorePatient.SP_GIVEN,
					USCorePatient.SP_NAME,
					USCorePatient.SP_PHONE,
					USCorePatient.SP_TELECOM,
					USCorePatient.SP_IDENTIFIER }) ReferenceAndListParam theSubjects,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = getTableName() + " comp";

		// Set up join statements.
		if (theSubjects != null || thePatients != null) {
			fromStatement += " join patient p on comp.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";
		}

		if (theDeathLocations != null || theDeathDate != null) {
			fromStatement += " join observation o on comp.resource->'subject'->>'reference' = o.resource->'subject'->>'reference'";
		}

		if (theDeathLocations != null) {
			fromStatement += " join location l on o.resource->'extension'->0->>'url' = 'http://hl7.org/fhir/us/vrdr/StructureDefinition/Observation-Location' " 
				+ "and o.resource->'extension'->0->'valueReference'->>'reference' = concat('Location/', l.resource->>'id')";
		}

		boolean returnAll = true;

		if (theSubjects != null || thePatients != null) {
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

		if (theDeathLocations != null) {
			fromStatement = constructFromStatementPath(fromStatement, "deathdate", "o.resource->'code'->'coding'");
			addToWhereParemters(whereParameters, "deathdate @> '{\"system\": \"http://loinc.org\", \"code\": \"81956-5\"}'::jsonb");
			String districtOrWhere = "";
			for (StringParam theDeathLocation : theDeathLocations.getValuesAsQueryTokens()) {
				if (districtOrWhere == null || districtOrWhere.isEmpty()) {
					districtOrWhere = "l.resource->'address'->>'district' like '%" + theDeathLocation.getValue() + "%'";
				} else {
					districtOrWhere += " or l.resource->'address'->>'district' like '%" + theDeathLocation.getValue() + "%'";
				}
			}

			whereParameters.add(districtOrWhere);
		}

		if (theDeathDate != null) {
			fromStatement = constructFromStatementPath(fromStatement, "deathdate", "o.resource->'code'->'coding'");
			addToWhereParemters(whereParameters, "deathdate @> '{\"system\": \"http://loinc.org\", \"code\": \"81956-5\"}'::jsonb");
			String deathDateWhere = "";
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
			String lowerBound = formatter.format(theDeathDate.getLowerBoundAsInstant());
			if (theDeathDate.getLowerBound() != null && !theDeathDate.getLowerBound().isEmpty()
				&& theDeathDate.getUpperBound() != null && !theDeathDate.getUpperBound().isEmpty()) {
				// We have both lower and upper.
				String upperBound = formatter.format(theDeathDate.getUpperBoundAsInstant());
				deathDateWhere = "o.resource->>'valueDateTime' >= '" + lowerBound 
					+ "' and o.resource->>'valueDateTime' <= '" + upperBound + "'";
			} else {
				if (ParamPrefixEnum.GREATERTHAN_OR_EQUALS == theDeathDate.getLowerBound().getPrefix()) {
					deathDateWhere = "o.resource->>'valueDateTime' >= " + "'" + lowerBound + "'";
				} else if (ParamPrefixEnum.GREATERTHAN == theDeathDate.getLowerBound().getPrefix()) {
					deathDateWhere = "o.resource->>'valueDateTime' > " + "'" + lowerBound + "'";
				} else if (ParamPrefixEnum.LESSTHAN_OR_EQUALS == theDeathDate.getLowerBound().getPrefix()) {
					deathDateWhere = "o.resource->>'valueDateTime' <= " + "'" + lowerBound + "'";
				} else if (ParamPrefixEnum.LESSTHAN == theDeathDate.getLowerBound().getPrefix()) {
					deathDateWhere = "o.resource->>'valueDateTime' < " + "'" + lowerBound + "'";
				} else {
					deathDateWhere = "o.resource->>'valueDateTime' = " + "'" + lowerBound + "'";
				}
			}

			whereParameters.add(deathDateWhere);
		}

		if (theOrTypes != null) {
			fromStatement = constructFromStatementPath(fromStatement, "types", "comp.resource->'type'->'coding'");
			String where = constructTypeWhereParameter(theOrTypes);
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}

			returnAll = false;
		}

		if (theDate != null) {
			String where = constructDateWhereParameter(theDate, "comp", "date");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}

			returnAll = false;
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll && (whereStatement == null || whereStatement.isEmpty())) {
			return null;
		}

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT comp.resource as resource FROM " + fromStatement + whereStatement;

		logger.debug("query count:" + queryCount + "\nquery:" + query);

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, null, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	/**
	 * This is the "read" operation. The "@Read" annotation indicates that this
	 * method supports the read and/or vread operation.
	 * <p>
	 * Read operations take a single parameter annotated with the {@link IdParam}
	 * paramater, and should return a single resource instance.
	 * </p>
	 * 
	 * @param theId The read operation takes one parameter, which must be of type
	 *              IdDt and must be annotated with the "@Read.IdParam" annotation.
	 * @return Returns a resource matching this identifier, or null if none exists.
	 */
	@Read()
	public IBaseResource readComposition(@IdParam IdType theId) {
		IBaseResource retVal = null;

		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	/**
	 * The "@Update" annotation indicates that this method supports replacing an
	 * existing resource (by ID) with a new instance of that resource.
	 * 
	 * @param theId      This is the ID of the patient to update
	 * @param thePatient This is the actual resource to save
	 * @return This method returns a "MethodOutcome"
	 */
	@Update()
	public MethodOutcome updateComposition(@IdParam IdType theId, @ResourceParam Composition theComposition) {
		validateResource(theComposition);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource updatedObservation = getFhirbaseMapping().update(theComposition, getResourceType());
			retVal.setId(updatedObservation.getIdElement());
			retVal.setResource(updatedObservation);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	private void validateResource(Composition theComposition) {
		OperationOutcome outcome = new OperationOutcome();
		CodeableConcept detailCode = new CodeableConcept();
		if (theComposition.getType().isEmpty()) {
			detailCode.setText("No type is provided.");
			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		Reference subjectReference = theComposition.getSubject();
		if (subjectReference == null || subjectReference.isEmpty()) {
			detailCode.setText("Subject cannot be empty");
			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		String subjectResource = subjectReference.getReferenceElement().getResourceType();
		if (subjectResource != null && !subjectResource.contentEquals("Patient")) {
			detailCode.setText("Subject (" + subjectResource + ") must be Patient");
			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}
	}

	private BundleEntryComponent addToSectAndEntryofDoc(Composition composition, String referenceUrl,
			Resource resource, boolean addToSection) {
		if (addToSection) {
			SectionComponent sectionComponent = composition.getSectionFirstRep();
			sectionComponent.addEntry(new Reference(referenceUrl));
		}
		
		BundleEntryComponent bundleEntry = new BundleEntryComponent();
		bundleEntry.setFullUrl(referenceUrl);
		bundleEntry.setResource(resource);

		return bundleEntry;
	}
	
	private Resource processReference(IGenericClient client, List<BundleEntryComponent> bundleEntries, 
			List<String> addedResource, List<String> addedPractitioner, Composition composition, Reference reference,
			boolean addToSection) {
		Resource resource = null;
		if (reference != null && !reference.isEmpty()) {
			String referenceId = reference.getReferenceElement().getValue();
			if (!addedResource.contains(referenceId)) {
				resource = (Resource) client.read()
						.resource(reference.getReferenceElement().getResourceType())
						.withId(reference.getReferenceElement().getIdPart()).encodedJson()
						.execute();
				bundleEntries.add(addToSectAndEntryofDoc(composition, referenceId, resource, addToSection));
				addedResource.add(referenceId);

				if (resource instanceof Practitioner && !addedPractitioner.contains(referenceId)) {
					addedPractitioner.add(resource.getIdElement().getIdPart());
				}
			}
		}
		
		return resource;
	}

	private void setupClientForAuth(IGenericClient client) {
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
	}

	@Search(queryName=CompositionResourceProvider.NQ_EVENT_DETAIL)
	public IBundleProvider searchByNamedQuery(
		@RequiredParam(name="case-id") TokenAndListParam theCaseIds,
		@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = getTableName() + " comp";
		fromStatement += " join procedure proc on comp.resource->'event'->'detail'->>'reference' = concat('Procedure/', proc.resource->>'id')";

		boolean returnAll = true;

		fromStatement = constructFromStatementPath(fromStatement, "identifiers", "proc.resource->'identifier'");
		for (TokenOrListParam theCaseIdAnd : theCaseIds.getValuesAsQueryTokens()) {
			String where = constructIdentifierWhereParameter(theCaseIdAnd);
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
			returnAll = false;
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll && (whereStatement == null || whereStatement.isEmpty())) {
			return null;
		}

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT comp.resource as resource FROM " + fromStatement + whereStatement;

		logger.debug("query count:" + queryCount + "\nquery:" + query);

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, null, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	@Operation(name = "$mdi-documents", idempotent = true, bundleType = BundleTypeEnum.SEARCHSET)
	public Bundle generateMdiDocumentOperation(RequestDetails theRequestDetails, 
			@IdParam(optional=true) IdType theCompositionId,
			@OperationParam(name = "id") UriOrListParam theIds, 
			@OperationParam(name = "decedent.address-city") StringAndListParam theAddressCities,
			@OperationParam(name = "decedent.address-postalcode") StringAndListParam theAddressPostalCodes,
			@OperationParam(name = "decedent.address-state") StringAndListParam theAddressStates,
			@OperationParam(name = "decedent.address-use") TokenAndListParam theAddressUses,
			@OperationParam(name = "decedent.birthdate") DateRangeParam theBirthDateRange,
			@OperationParam(name = "decedent.email") TokenAndListParam theEmails,
			@OperationParam(name = "decedent.family") StringAndListParam theFamilies,
			@OperationParam(name = "decedent.gender") TokenAndListParam theGenders,
			@OperationParam(name = "decedent.given") StringAndListParam theGivens,
			@OperationParam(name = "decedent.identifier") TokenAndListParam theIdentifiers,
			@OperationParam(name = "decedent.name") StringAndListParam theNames,
			@OperationParam(name = "decedent.phone") TokenAndListParam thePhones,
			@OperationParam(name = "decedent.telecom") TokenAndListParam theTelecoms,
			@OperationParam(name = "vrdr-death-certification.identifier") TokenAndListParam theCaseIds,
			@OperationParam(name = "vrdr-death-location.district") StringOrListParam theDeathLocations,
			@OperationParam(name = "vrdr-death-date.value-date", max = 2) DateOrListParam theProfileDeathDateRange) {
				
		String myFhirServerBase = theRequestDetails.getFhirServerBase();
		IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
		setupClientForAuth(client);

		int totalSize = 0;

		Bundle retBundle = new Bundle();
		if (theCompositionId != null) {
			// if we have the composition id, then all search parameters will be ignored.
			return client
				.operation()
				.onInstance(theCompositionId)
				.named("$document")
				.withNoParameters(Parameters.class)
				.returnResourceType(Bundle.class)
				.execute();
		}

		retBundle.setType(BundleType.SEARCHSET);
		retBundle.setId(UUID.randomUUID().toString());
		retBundle.setTotal(totalSize);
		BundleLinkComponent bundleLinkComponent = new BundleLinkComponent(new StringType("self"), new UriType (theRequestDetails.getCompleteUrl()));
		retBundle.addLink(bundleLinkComponent);

		if (theIds != null) {
			// Composition ID tokens. This is to retrieve documents for the IDs.
			// This will ignore other search parameters.
			for (UriParam theId: theIds.getValuesAsQueryTokens()) {
				String id = theId.getValue();
				Bundle compositionBundle = client
					.operation().onInstance(new IdType("Composition", id))
					.named("$document")
					.withNoParameters(Parameters.class)
					.returnResourceType(Bundle.class)
					.execute();
				BundleEntryComponent entryComponent = new BundleEntryComponent();
				entryComponent.setFullUrl(compositionBundle.getId());
				entryComponent.setResource(compositionBundle);
				retBundle.addEntry(entryComponent);

				totalSize++;
			}

			retBundle.setTotal(totalSize);

			return retBundle;
		}
		
		Bundle compositionsBundle = null;
		IQuery<IBaseBundle> query = client.search().forResource(Composition.class);

		if (theCaseIds != null) {
			String namedQuery = "";
			String caseIdOr = null;
			String caseIdsAnd = null;
			for (TokenOrListParam theCaseIdAnd: theCaseIds.getValuesAsQueryTokens()) {
				caseIdOr = null;
				for (TokenParam theIdentifier : theCaseIdAnd.getValuesAsQueryTokens()) {
					if (caseIdOr == null) {
						caseIdOr = theIdentifier.getSystem() + "|" + theIdentifier.getValue();
					} else {
						caseIdOr = caseIdOr.concat("," + theIdentifier.getSystem() + "|" + theIdentifier.getValue());
					}
				}
				if (caseIdsAnd == null) {
					caseIdsAnd = "case-id=" + caseIdOr;
				} else {
					caseIdsAnd = "&case-id=" + caseIdOr;
				}
			}

			if (caseIdsAnd != null && !caseIdsAnd.isEmpty()) {
				namedQuery = "Composition?_query=" + CompositionResourceProvider.NQ_EVENT_DETAIL + "&" +caseIdsAnd;
				compositionsBundle = client.search()
					.byUrl(namedQuery)
					.returnBundle(Bundle.class)
					.execute();
			}
		} else {
			boolean shouldQuery = false;

			List<String> addressCitySearchParams = new ArrayList<String>();
			if (theAddressCities != null) {
				for (StringOrListParam theAddressCitiesAnd : theAddressCities.getValuesAsQueryTokens()) {
					addressCitySearchParams.clear();
					for (StringParam theAddressCity : theAddressCitiesAnd.getValuesAsQueryTokens()) {
						addressCitySearchParams.add(theAddressCity.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.ADDRESS_CITY.matches().values(addressCitySearchParams)));
					shouldQuery = true;
				}
			}

			List<String> addressPostalCodeSearchParams = new ArrayList<String>();
			if (theAddressPostalCodes != null) {
				for (StringOrListParam theAddressPostalCodesAnd : theAddressPostalCodes.getValuesAsQueryTokens()) {
					addressPostalCodeSearchParams.clear();
					for (StringParam theAddressPostalCode : theAddressPostalCodesAnd.getValuesAsQueryTokens()) {
						addressPostalCodeSearchParams.add(theAddressPostalCode.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.ADDRESS_POSTALCODE.matches().values(addressPostalCodeSearchParams)));
					shouldQuery = true;
				}
			}

			List<String> addressStateSearchParams = new ArrayList<String>();
			if (theAddressStates != null) {
				for (StringOrListParam theAddressStatesAnd : theAddressStates.getValuesAsQueryTokens()) {
					addressStateSearchParams.clear();
					for (StringParam theAddressState : theAddressStatesAnd.getValuesAsQueryTokens()) {
						addressStateSearchParams.add(theAddressState.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.ADDRESS_STATE.matches().values(addressStateSearchParams)));
					shouldQuery = true;
				}
			}

			List<String> addressUseSearchParams = new ArrayList<String>();
			if (theAddressUses != null) {
				for (TokenOrListParam theAddressUseAnd: theAddressUses.getValuesAsQueryTokens()) {
					addressUseSearchParams.clear();
					for (TokenParam theAddressUse : theAddressUseAnd.getValuesAsQueryTokens()) {
						addressUseSearchParams.add(theAddressUse.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.ADDRESS_USE.exactly().codes(addressCitySearchParams)));
					shouldQuery = true;
				}
			}

			if (theBirthDateRange != null) {
				DateParam lowerDateParam = theBirthDateRange.getLowerBound();
				DateParam upperDateParam = theBirthDateRange.getUpperBound();
				if (lowerDateParam != null && upperDateParam != null) {
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.afterOrEquals().day(lowerDateParam.getValue())))
						.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.beforeOrEquals().day(upperDateParam.getValue())));
					shouldQuery = true;
				} else if (lowerDateParam != null) {
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.afterOrEquals().day(lowerDateParam.getValue())));
					shouldQuery = true;
				} else if (upperDateParam != null) {
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.beforeOrEquals().day(upperDateParam.getValue())));
					shouldQuery = true;
				}
			}

			List<String> emailSearchParams = new ArrayList<String>();
			if (theEmails != null) {
				for (TokenOrListParam theEmailAnd: theEmails.getValuesAsQueryTokens()) {
					emailSearchParams.clear();
					for (TokenParam theEmail : theEmailAnd.getValuesAsQueryTokens()) {
						emailSearchParams.add(theEmail.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.EMAIL.exactly().codes(emailSearchParams)));
					shouldQuery = true;
				}
			}

			List<String> familySearchParams = new ArrayList<String>();
			if (theFamilies != null) {
				for (StringOrListParam theFamilyAnd : theFamilies.getValuesAsQueryTokens()) {
					familySearchParams.clear();
					for (StringParam theFamily : theFamilyAnd.getValuesAsQueryTokens()) {
						familySearchParams.add(theFamily.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.FAMILY.matches().values(familySearchParams)));
					shouldQuery = true;
				}
			}

			List<String> genderSearchParams = new ArrayList<String>();
			if (theGenders != null) {
				for (TokenOrListParam theGenderAnd: theGenders.getValuesAsQueryTokens()) {
					genderSearchParams.clear();
					for (TokenParam theGender : theGenderAnd.getValuesAsQueryTokens()) {
						genderSearchParams.add(theGender.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.GENDER.exactly().codes(genderSearchParams)));
					shouldQuery = true;
				}
			}

			List<String> givenSearchParams = new ArrayList<String>();
			if (theGivens != null) {
				for (StringOrListParam theGivenAnd : theGivens.getValuesAsQueryTokens()) {
					givenSearchParams.clear();
					for (StringParam theGiven : theGivenAnd.getValuesAsQueryTokens()) {
						givenSearchParams.add(theGiven.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.GIVEN.matches().values(givenSearchParams)));
					shouldQuery = true;
				}
			}

			List<String> identifierSearchParams = new ArrayList<String>();
			if (theIdentifiers != null) {
				for (TokenOrListParam theIdentifierAnd: theIdentifiers.getValuesAsQueryTokens()) {
					identifierSearchParams.clear();
					for (TokenParam theIdentifier : theIdentifierAnd.getValuesAsQueryTokens()) {
						identifierSearchParams.add(theIdentifier.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.IDENTIFIER.exactly().codes(identifierSearchParams)));
					shouldQuery = true;
				}
			}

			List<String> nameSearchParams = new ArrayList<String>();
			if (theNames != null) {
				for (StringOrListParam theNamesAnd: theNames.getValuesAsQueryTokens()) {
					nameSearchParams.clear();
					for (StringParam theName : theNamesAnd.getValuesAsQueryTokens()) {
						String nameValue = theName.getValue();
						if ("ERROR:DEMO-ERROR".equals(nameValue)) {
							throwSimulatedOO(nameValue);
						}
						nameSearchParams.add(nameValue);
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.NAME.matches().values(nameSearchParams)));
					shouldQuery = true;
				}

			}

			List<String> phoneSearchParams = new ArrayList<String>();
			if (thePhones != null) {
				for (TokenOrListParam thePhoneAnd: thePhones.getValuesAsQueryTokens()) {
					phoneSearchParams.clear();
					for (TokenParam thePhone : thePhoneAnd.getValuesAsQueryTokens()) {
						phoneSearchParams.add(thePhone.getValue());
					}
					query = query.and(Composition.PATIENT.hasChainedProperty(Patient.PHONE.exactly().codes(phoneSearchParams)));
					shouldQuery = true;
				}

			}

			List<String> telecomSearchParams = new ArrayList<String>();
			if (theTelecoms != null) {
				for (TokenOrListParam theTelecomAnd: theTelecoms.getValuesAsQueryTokens()) {
					telecomSearchParams.clear();
					String systemName = "";
					for (TokenParam theTelecom : theTelecomAnd.getValuesAsQueryTokens()) {
						if (systemName.isEmpty()) {
							systemName = theTelecom.getSystem();
						}
						telecomSearchParams.add(theTelecom.getValue());
					}

					if (systemName.isEmpty()) {
						query = query.and(Composition.PATIENT.hasChainedProperty(Patient.TELECOM.exactly().codes(telecomSearchParams)));
					} else {
						query = query.and(Composition.PATIENT.hasChainedProperty(Patient.TELECOM.exactly().systemAndValues(systemName, telecomSearchParams)));
					}

					shouldQuery = true;
				}

			}

			// Death Location is ONLY referenced from Death Date Observation. 
			// Thus, we need to search Death Date and Location at the same time.
			List<String> deathLocationSearchParams = new ArrayList<String>();
			if (theDeathLocations != null) {
				for (StringParam theDeathLocation : theDeathLocations.getValuesAsQueryTokens()) {
					deathLocationSearchParams.add(theDeathLocation.getValue());
				}
				query = query.and(
					(new ca.uhn.fhir.rest.gclient.StringClientParam(CompositionResourceProvider.SP_DEATH_LOCATION))
						.matches()
						.values(deathLocationSearchParams)
					);

				shouldQuery = true;
			}

			// Death Date 
			if (theProfileDeathDateRange != null) {
				List<DateParam> dates = theProfileDeathDateRange.getValuesAsQueryTokens();
				for (DateParam date : dates) {
					logger.debug("Date Received: " + date.getValueAsString());
					ParamPrefixEnum prefix = date.getPrefix();
					if (prefix == null) {
						query = query.and((new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
							.exactly().day(date.getValue()));
					} else {
						if (ParamPrefixEnum.GREATERTHAN_OR_EQUALS == prefix) {
							query = query.and((new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
								.afterOrEquals().day(date.getValue()));
						} else if (ParamPrefixEnum.GREATERTHAN == prefix) {
							query = query.and((new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
								.after().day(date.getValue()));
						} else if (ParamPrefixEnum.LESSTHAN_OR_EQUALS == prefix) {
							query = query.and((new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
								.beforeOrEquals().day(date.getValue()));
						} else if (ParamPrefixEnum.LESSTHAN == prefix) {
							query = query.and((new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
								.before().day(date.getValue()));
						} else {
							query = query.and((new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
								.exactly().day(date.getValue()));
						}						
					}

					shouldQuery = true;
				}
			}

			if (shouldQuery) {
				compositionsBundle = query.returnBundle(Bundle.class).execute();
			}
		}

		if (compositionsBundle != null && !compositionsBundle.isEmpty()) {
			List<BundleEntryComponent> entries = compositionsBundle.getEntry();
			for (BundleEntryComponent entry : entries) {
				String compositionId = entry.getResource().getIdElement().getIdPart();

				Bundle compositionBundle = client
					.operation().onInstance(new IdType("Composition", compositionId))
					.named("$document")
					.withNoParameters(Parameters.class)
					.returnResourceType(Bundle.class)
					.execute();
				BundleEntryComponent entryComponent = new BundleEntryComponent();
				entryComponent.setFullUrl(compositionBundle.getId());
				entryComponent.setResource(compositionBundle);
				retBundle.addEntry(entryComponent);

				totalSize++;
			}

			retBundle.setTotal(totalSize);
		}

		return retBundle;
	}
	
	@Operation(name = "$document", idempotent = true, bundleType = BundleTypeEnum.DOCUMENT)
	public Bundle generateDocumentOperation(RequestDetails theRequestDetails, 
			@IdParam IdType theCompositionId,
			@OperationParam(name = "id") UriParam theIdUri, 
			@OperationParam(name = "persist") BooleanType thePersist,
			@OperationParam(name = "graph") UriParam theGraph) {

		OperationOutcome outcome = new OperationOutcome();
		if (thePersist != null && !thePersist.isEmpty() && thePersist.getValue().booleanValue()) {
			// We can't store this bundler.
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("This server do not support Bundle persist"));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		if (theCompositionId == null || theCompositionId.isEmpty()) {
			// see if the id is coming as an input parameter.
			if (theIdUri == null || theIdUri.isEmpty()) {
				// We do not support getting all Document.
				outcome.addIssue().setSeverity(IssueSeverity.WARNING).setDetails(
						(new CodeableConcept()).setText("id is required. We do not support getting all documents"));
				throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
			} else {
				String compositionId = null;
				compositionId = theIdUri.getValue();
				if (compositionId.startsWith("http")) {
					// We do not support the external composition.
					outcome.addIssue().setSeverity(IssueSeverity.WARNING).setDetails(
							(new CodeableConcept()).setText("We do not support external composition documents"));
					throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
				}

				theCompositionId = new IdType(compositionId);
			}
		}

		Composition composition = (Composition) readComposition(theCompositionId);
		if (composition == null || composition.isEmpty()) {
			// We must have valid composition in the server that matches the id parameter.
			// If it's not available, then we return null.
			return null;
		}

		CodeableConcept myType = composition.getType();

		if (myType.isEmpty()) {
			// We can't generate a document without type.
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("This composition has no type"));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		Coding typeCoding = myType.getCodingFirstRep();
		if (typeCoding.isEmpty()) {
			// We must have coding.
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("This composition type has no coding specified"));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		String typeSystem = typeCoding.getSystem();
		String typeCode = typeCoding.getCode();

		List<BundleEntryComponent> bundleEntries = new ArrayList<BundleEntryComponent>();

		// First entry must be composition.
		BundleEntryComponent bundleEntry = new BundleEntryComponent();
		bundleEntry.setFullUrlElement(composition.getIdElement());
		bundleEntry.setResource(composition);
		bundleEntries.add(bundleEntry);

		String myFhirServerBase = theRequestDetails.getFhirServerBase();
		IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
		setupClientForAuth(client);

		String metaProfile = "";
		if ("http://loinc.org".equalsIgnoreCase(typeSystem) && "64297-5".equalsIgnoreCase(typeCode)) {
			// This is a death certificate document. We need to add full resources in the
			// section entries
			// to the resources.
			metaProfile = "http://hl7.org/fhir/us/vrdr/StructureDefinition/VRDR-Death-Certificate-Document";

			// The composition section is empty. It means that VRDR has never been
			// generated. We generate it here and persist it.
			// There is no order here. But, put patient first for human eye.
			// If composition section is not empty, we honor that and do not add resources related to
			// death certificate. But, we add them to entry.
			boolean addToSection = true;
			
			List<String> addedResource = new ArrayList<String>();
			List<String> addedPractitioner = new ArrayList<String>();

			for (SectionComponent section : composition.getSection()) {
				for (Reference reference : section.getEntry()) {
					String referenceId = reference.getReferenceElement().getValue();
					addToSection = false;

					if (!addedResource.contains(referenceId)) {
						// get the reference.
						Resource response = (Resource) client.read()
								.resource(reference.getReferenceElement().getResourceType())
								.withId(reference.getReferenceElement().getIdPart()).encodedJson().execute();
						if (response == null || response.isEmpty()) {
							outcome.addIssue().setSeverity(IssueSeverity.ERROR)
									.setDetails((new CodeableConcept())
											.setText("resource (" + reference.getReferenceElement().getValue()
													+ ") in Composition section not found"));
							throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
						}
						bundleEntries.add(addToSectAndEntryofDoc(composition, referenceId, response, addToSection));
						addedResource.add(referenceId);
	
						if (response instanceof Practitioner && !addedPractitioner.contains(referenceId)) {
							addedPractitioner.add(response.getIdElement().getIdPart());
						}
					}					
				}
			}
			
			String patientId = composition.getSubject().getReferenceElement().getIdPart();

			if (!addedResource.contains("Patient/" + patientId)) {
				Patient patient = client.read().resource(Patient.class).withId(patientId).encodedJson().execute();
				bundleEntries.add(addToSectAndEntryofDoc(composition, "Patient/" + patientId, patient, addToSection));
				addedResource.add("Patient/" + patientId);
			}

			// Add all observations
			Bundle obsBundle = client.search().forResource(Observation.class)
					.where(Observation.SUBJECT.hasId(patientId)).returnBundle(Bundle.class).execute();
			List<BundleEntryComponent> obsEntries = obsBundle.getEntry();
			for (BundleEntryComponent obsEntry : obsEntries) {
				Observation observation = (Observation) obsEntry.getResource();
				if (observation != null && !observation.isEmpty()) {
					// First add this resource to section and entry of this bundle document.
					if (!addedResource.contains("Observation/" + observation.getIdElement().getIdPart())) {
						bundleEntries.add(addToSectAndEntryofDoc(composition,
								"Observation/" + observation.getIdElement().getIdPart(), observation, addToSection));
						addedResource.add("Observation/" + observation.getIdElement().getIdPart());
					}

					// find out any resources that referenced by this observation.
					// First locations in the extension
					List<Extension> obsExts = observation.getExtension();
					for (Extension obsExt : obsExts) {
						if (obsExt != null && !obsExt.isEmpty()) {
							Type value = obsExt.getValue();
							if (value instanceof Reference) {
								Reference reference = (Reference) value;
								String referenceId = reference.getReferenceElement().getValue();
								if (!addedResource.contains(referenceId)) {
									Resource resource = (Resource) client.read()
											.resource(reference.getReferenceElement().getResourceType())
											.withId(reference.getReferenceElement().getIdPart()).encodedJson()
											.execute();
									bundleEntries.add(addToSectAndEntryofDoc(composition, referenceId, resource, addToSection));
									addedResource.add(referenceId);
								}
							}
						}
					}

					// get performer (practitioner)
					List<Reference> obsPerformers = observation.getPerformer();
					for (Reference reference : obsPerformers) {
						processReference(client, bundleEntries, addedResource, addedPractitioner, composition, reference, addToSection);
					}
				}
			}

			// Add Conditions
			Bundle condBundle = client.search().forResource(Condition.class)
					.where(Condition.SUBJECT.hasId(patientId)).returnBundle(Bundle.class).execute();
			List<BundleEntryComponent> condEntries = condBundle.getEntry();
			for (BundleEntryComponent condEntry : condEntries) {
				Condition condition = (Condition) condEntry.getResource();
				if (condition != null && !condition.isEmpty()) {
					if (!addedResource.contains("Condition/" + condition.getIdElement().getIdPart())) {
						bundleEntries.add(addToSectAndEntryofDoc(composition,
								"Condition/" + condition.getIdElement().getIdPart(), condition, addToSection));
						addedResource.add("Condition/" + condition.getIdElement().getIdPart());
					}

					// Get asserter
					Reference reference = condition.getAsserter();
					processReference(client, bundleEntries, addedResource, addedPractitioner, composition, reference, addToSection);
				}
			}

			// Procedure
			Bundle relProcedureBundle = client.search().forResource(Procedure.class)
					.where(Procedure.PATIENT.hasId(patientId)).returnBundle(Bundle.class).execute();
			List<BundleEntryComponent> relProcedureEntries = relProcedureBundle.getEntry();
			for (BundleEntryComponent relProcedureEntry : relProcedureEntries) {
				Procedure procedure = (Procedure) relProcedureEntry.getResource();
				if (procedure != null && !procedure.isEmpty()) {
					// First add this resource to section and entry of this bundle document.
					if (!addedResource.contains("Procedure/" + procedure.getIdElement().getIdPart())) {
						bundleEntries.add(addToSectAndEntryofDoc(composition,
								"Procedure/" + procedure.getIdElement().getIdPart(), procedure, addToSection));
						addedResource.add("Procedure/" + procedure.getIdElement().getIdPart());
					}
					
					// Get performer.actor
					ProcedurePerformerComponent performer = procedure.getPerformerFirstRep();
					if (!performer.isEmpty()) {
						Reference reference = performer.getActor();
						processReference(client, bundleEntries, addedResource, addedPractitioner, composition, reference, addToSection);
					}
				}
			}


			// Practitioner referenced resources
			for (String idPart : addedPractitioner) {
				// Get List (this is Cause of Death pathway)
				Bundle listBundle = client.search().forResource(ListResource.class)
						.where(ListResource.SOURCE.hasId("Practitioner/" + idPart)).returnBundle(Bundle.class)
						.execute();
				List<BundleEntryComponent> entries = listBundle.getEntry();
				for (BundleEntryComponent entry : entries) {
					ListResource list = (ListResource) entry.getResource();
					if (list != null && !list.isEmpty()) {
						if (!addedResource.contains("List/" + list.getIdElement().getIdPart())) {
							bundleEntries.add(addToSectAndEntryofDoc(composition,
									"List/" + list.getIdElement().getIdPart(), list, addToSection));
							addedResource.add("List/" + list.getIdElement().getIdPart());
						}
					}
				}

				// Get PractitionerRole
				Bundle practRoleBundle = client.search().forResource(PractitionerRole.class)
						.where(PractitionerRole.PRACTITIONER.hasId(idPart)).returnBundle(Bundle.class).execute();
				List<BundleEntryComponent> practRoleEntries = practRoleBundle.getEntry();
				for (BundleEntryComponent practRoleEntry : practRoleEntries) {
					PractitionerRole practRole = (PractitionerRole) practRoleEntry.getResource();
					if (practRole != null && !practRole.isEmpty()) {
						if (!addedResource.contains("PractitionerRole/" + practRole.getIdElement().getIdPart())) {
							bundleEntries.add(addToSectAndEntryofDoc(composition,
									"PractitionerRole/" + practRole.getIdElement().getIdPart(), practRole, addToSection));
							addedResource.add("PractitionerRole/" + practRole.getIdElement().getIdPart());

							// add Organization (Funeral Home) if not added.
							Reference reference = practRole.getOrganization();
							String referenceId = reference.getReferenceElement().getValue();
							if (reference != null && !reference.isEmpty() && !addedResource.contains(referenceId)) {
								Resource resource = (Resource) client.read()
										.resource(reference.getReferenceElement().getResourceType())
										.withId(reference.getReferenceElement().getIdPart()).encodedJson()
										.execute();
								bundleEntries.add(addToSectAndEntryofDoc(composition, referenceId, resource, addToSection));
								addedResource.add(referenceId);
							}
						}
					}
				}
			}
			
			// List resource, which is a pathway for cause of death, must be included from
			// certifier. However, if this is missed and the list has subject, then use it to 
			// include here. 
			Bundle listBundle = client.search().forResource(ListResource.class)
					.where(ListResource.PATIENT.hasId(patientId)).returnBundle(Bundle.class).execute();
			List<BundleEntryComponent> listEntries = listBundle.getEntry();
			for (BundleEntryComponent entry : listEntries) {
				ListResource list = (ListResource) entry.getResource();
				if (list != null && !list.isEmpty()) {
					if (!addedResource.contains("List/" + list.getIdElement().getIdPart())) {
						bundleEntries.add(addToSectAndEntryofDoc(composition,
								"List/" + list.getIdElement().getIdPart(), list, addToSection));
						addedResource.add("List/" + list.getIdElement().getIdPart());
					}
				}
			}

			// RelatedPerson
			Bundle relPersonBundle = client.search().forResource(RelatedPerson.class)
					.where(RelatedPerson.PATIENT.hasId(patientId)).returnBundle(Bundle.class).execute();
			List<BundleEntryComponent> relPersonEntries = relPersonBundle.getEntry();
			for (BundleEntryComponent relPersonEntry : relPersonEntries) {
				RelatedPerson relatedPerson = (RelatedPerson) relPersonEntry.getResource();
				if (relatedPerson != null && !relatedPerson.isEmpty()) {
					// First add this resource to section and entry of this bundle document.
					if (!addedResource.contains("RelatedPerson/" + relatedPerson.getIdElement().getIdPart())) {
						bundleEntries.add(addToSectAndEntryofDoc(composition,
								"RelatedPerson/" + relatedPerson.getIdElement().getIdPart(), relatedPerson, addToSection));
						addedResource.add("RelatedPerson/" + relatedPerson.getIdElement().getIdPart());
					}
				}
			}
		} else if ("http://loinc.org".equalsIgnoreCase(typeSystem) && "11502-2".equalsIgnoreCase(typeCode)) {
			for (SectionComponent section : composition.getSection()) {
				for (Reference reference : section.getEntry()) {
					// get the reference.
					Resource response = (Resource) client.read()
							.resource(reference.getReferenceElement().getResourceType())
							.withId(reference.getReferenceElement().getIdPart()).encodedJson().execute();
					if (response == null || response.isEmpty()) {
						outcome.addIssue().setSeverity(IssueSeverity.ERROR)
								.setDetails((new CodeableConcept())
										.setText("resource (" + reference.getReferenceElement().getValue()
												+ ") in Composition section not found"));
						throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
					}

					bundleEntry = new BundleEntryComponent();
					bundleEntry.setFullUrl(reference.getReferenceElement().getValue());
					bundleEntry.setResource(response);
					bundleEntries.add(bundleEntry);
				}
			}
		} else {
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("This composition type document is not supported."));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		Bundle retBundle = new Bundle();

		// This is generate Document operation. Thus, type must be Document.
		retBundle.setType(Bundle.BundleType.DOCUMENT);

		if (!metaProfile.isEmpty()) {
			Meta meta = retBundle.getMeta();
			meta.addProfile(metaProfile);
			retBundle.setMeta(meta);
		}

		retBundle.setEntry(bundleEntries);
		retBundle.setId(UUID.randomUUID().toString());

		return retBundle;
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

			String myQuery = query;
			if (toIndex - fromIndex > 0) {
				myQuery += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			logger.debug("calling database: "+myQuery);
			try {
				retVal.addAll(getFhirbaseMapping().search(myQuery, getResourceType()));
			} catch (SQLException e) {
				e.printStackTrace();
			}

			return retVal;
		}
	}
}
