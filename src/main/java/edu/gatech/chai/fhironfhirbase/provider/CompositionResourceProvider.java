package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import jakarta.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Composition.CompositionAttesterComponent;
import org.hl7.fhir.r4.model.Composition.CompositionEventComponent;
import org.hl7.fhir.r4.model.Composition.CompositionRelatesToComponent;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.MessageHeader;
import org.hl7.fhir.r4.model.MessageHeader.MessageSourceComponent;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Practitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.model.api.annotation.SearchParamDefinition;
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
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.param.DateOrListParam;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.DateRangeParam;
import ca.uhn.fhir.rest.param.ParamPrefixEnum;
import ca.uhn.fhir.rest.param.ParameterUtil;
import ca.uhn.fhir.rest.param.ReferenceAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenAndListParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.param.UriParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;
import edu.gatech.chai.fhironfhirbase.utilities.MdiProfileUtil;
import edu.gatech.chai.fhironfhirbase.utilities.OperationUtil;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;

@Service
@Scope("prototype")
public class CompositionResourceProvider extends BaseResourceProvider {
	private static final Logger logger = LoggerFactory.getLogger(CompositionResourceProvider.class);
	public static final String NQ_EVENT_DETAIL = "event-detail";

	/**
	 * Search parameter: <b>death-date-pronounced</b>
	 * <p>
	 * Description: <b>Actual Date of Pronounced Death</b><br>
	 * Type: <b>date</b><br>
	 * Path: <b>Observation.component</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name = "death-date-pronounced", path = "Observation.valueDateTime", description = "Date of Pronounced Death", type = "date")
	public static final String SP_DEATH_DATE_PRONOUNCED = "death-date-pronounced";
	/**
	 * <b>Fluent Client</b> search parameter constant for
	 * <b>death-date-pronounced</b>
	 * <p>
	 * Description: <b>Actual Date of Pronounced Death</b><br>
	 * Type: <b>date</b><br>
	 * Path: <b>Observation.component</b><br>
	 * </p>
	 */
	public static final ca.uhn.fhir.rest.gclient.DateClientParam DEATH_DATE_PRONOUNCED = new ca.uhn.fhir.rest.gclient.DateClientParam(
			SP_DEATH_DATE_PRONOUNCED);

	/**
	 * Search parameter: <b>death-date</b>
	 * <p>
	 * Description: <b>Date of Death</b><br>
	 * Type: <b>date</b><br>
	 * Path: <b>Observation.valueDateTime or Observation.component</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name = "death-date", path = "Observation.component", description = "Date of Death", type = "date")
	public static final String SP_DEATH_DATE = "death-date";
	/**
	 * <b>Fluent Client</b> search parameter constant for
	 * <b>death-date-pronounced</b>
	 * <p>
	 * Description: <b>Date of Death</b><br>
	 * Type: <b>date</b><br>
	 * Path: <b>Observation.valueDateTime or Observation.component</b><br>
	 * </p>
	 */
	public static final ca.uhn.fhir.rest.gclient.DateClientParam DEATH_DATE = new ca.uhn.fhir.rest.gclient.DateClientParam(
			SP_DEATH_DATE);

	/**
	 * Search parameter: <b>death-location</b>
	 * <p>
	 * Description: <b>District of death location</b><br>
	 * Type: <b>string</b><br>
	 * Path: <b>Composition.death-location</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name = "death-location", path = "Location.name", description = "District of death location", type = "string")
	public static final String SP_DEATH_LOCATION = "death-location";
	/**
	 * <b>Fluent Client</b> search parameter constant for <b>edrs-file-number</b>
	 * <p>
	 * Description: <b>District of death location</b><br>
	 * Type: <b>string</b><br>
	 * Path: <b>Composition.death-location</b><br>
	 * </p>
	 */
	public static final ca.uhn.fhir.rest.gclient.StringClientParam DEATH_LOCATION = new ca.uhn.fhir.rest.gclient.StringClientParam(
			SP_DEATH_LOCATION);

	/**
	 * Search parameter: <b>tracking-number</b>
	 * <p>
	 * Description: <b>A composition extension identifier for
	 * tracking-number</b><br>
	 * Type: <b>token</b><br>
	 * Path: <b>Composition.tracking-number</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name = "tracking-number", path = "Composition.extension-tracking-numbers", description = "Extension Trakcing Number for Case File", type = "token")
	public static final String SP_TRACKING_NUMBER = "tracking-number";
	/**
	 * <b>Fluent Client</b> search parameter constant for <b>tracking-number</b>
	 * <p>
	 * Description: <b>A composition extension identifier for
	 * tracking-number</b><br>
	 * Type: <b>token</b><br>
	 * Path: <b>Composition.tracking-number</b><br>
	 * </p>
	 */
	public static final ca.uhn.fhir.rest.gclient.TokenClientParam TRACKING_NUMBER = new ca.uhn.fhir.rest.gclient.TokenClientParam(
			SP_TRACKING_NUMBER);

	/**
	 * Search parameter: <b>manner-of-death</b>
	 * <p>
	 * Description: <b>Composition has a reference to the manner of death
	 * observation. Search observation </b><br>
	 * Type: <b>token</b><br>
	 * Path: <b>Composition.tracking-number</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name = "manner-of-death", path = "Observation.valueCodeableConcept", description = "Manner of Death Concept Value for the Decedent", type = "token")
	public static final String SP_MANNER_OF_DEATH = "manner-of-death";
	/**
	 * <b>Fluent Client</b> search parameter constant for <b>manner-of-death</b>
	 * <p>
	 * Description: <b>A observation value for manner-of-death</b><br>
	 * Type: <b>token</b><br>
	 * Path: <b>Observation.valueCodeableConcept</b><br>
	 * </p>
	 */
	public static final ca.uhn.fhir.rest.gclient.TokenClientParam MANNER_OF_DEATH = new ca.uhn.fhir.rest.gclient.TokenClientParam(
			SP_MANNER_OF_DEATH);

	public CompositionResourceProvider(FhirContext ctx) {
		super(ctx);

		setTableName(CompositionResourceProvider.getType().toLowerCase());
		setMyResourceType(CompositionResourceProvider.getType());
	}

	@PostConstruct
	private void postConstruct() {
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
			@OptionalParam(name = CompositionResourceProvider.SP_DEATH_DATE_PRONOUNCED) DateOrListParam thePronouncedDeathDate,
			@OptionalParam(name = CompositionResourceProvider.SP_TRACKING_NUMBER) TokenOrListParam theTrackingNumber,
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
			// join patient and composition subject tables
			fromStatement += " join patient p on comp.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";
		}

		boolean returnAll = true;

		if (theSubjects != null || thePatients != null) {
			String updatedFromStatement = constructFromWherePatients(fromStatement, whereParameters, theSubjects);
			if (updatedFromStatement.isEmpty()) {
				// This means that we have unsupported resource. Since this is to search, we
				// should discard all and
				// return null.
				return null;
			}
			fromStatement = updatedFromStatement;

			updatedFromStatement = constructFromWherePatients(fromStatement, whereParameters, thePatients);
			if (updatedFromStatement.isEmpty()) {
				// This means that we have unsupported resource. Since this is to search, we
				// should discard all and
				// return null.
				return null;
			}
			fromStatement = updatedFromStatement;

			returnAll = false;
		}

		if (theDeathLocations != null) {
			fromStatement = constructFromStatementPath(fromStatement, "sections", "comp.resource->'section'");
			fromStatement = constructFromStatementPath(fromStatement, "entries", "sections->'entry'");
			fromStatement += " join location l on entries->>'reference' = concat('Location/', l.id)";
			fromStatement = constructFromStatementPath(fromStatement, "codings", "sections->'code'->'coding'");

			// We only want a section for circumstance where we keep death location
			// reference.
			whereParameters.add(
					"codings @> '{\"code\": \"circumstances\", \"system\": \"" + MdiProfileUtil.CS_MDI_CODES + "\"}'");

			String districtOrWhere = null;
			for (StringParam theDeathLocation : theDeathLocations.getValuesAsQueryTokens()) {
				String districtOrWhere_ = "lower(l.resource->'address'->>'city') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				districtOrWhere_ += " or lower(l.resource->'address'->>'state') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				districtOrWhere_ += " or lower(l.resource->'address'->>'district') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				districtOrWhere_ += " or lower(l.resource->'address'->>'postalCode') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				districtOrWhere_ += " or lower(l.resource->'address'->>'country') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				if (districtOrWhere == null || districtOrWhere.isEmpty()) {
					districtOrWhere = districtOrWhere_;
				} else {
					districtOrWhere += " or " + districtOrWhere_;
				}
			}

			if (districtOrWhere != null && !districtOrWhere.isBlank()) {
				whereParameters.add(districtOrWhere);
			}
		}

		// Death Date Pronounced
		if (thePronouncedDeathDate != null) {
			fromStatement = constructFromStatementPath(fromStatement, "sections", "comp.resource->'section'");
			fromStatement = constructFromStatementPath(fromStatement, "entries", "sections->'entry'");
			fromStatement += " join observation o on entries->>'reference' = concat('Observation/', o.id)";
			fromStatement = constructFromStatementPath(fromStatement, "codings", "sections->'code'->'coding'");
			fromStatement += ", jsonb_array_elements(o.resource->'component') component, jsonb_array_elements(component->'code'->'coding') component_codings";
			fromStatement = constructFromStatementPath(fromStatement, "deathdate", "o.resource->'code'->'coding'");

			// We only want a section for jurisdiction where we keep death date reference.
			whereParameters.add(
					"codings @> '{\"code\": \"jurisdiction\", \"system\": \"" + MdiProfileUtil.CS_MDI_CODES + "\"}'");

			// we want Death Date observation. So, check the code of Observations and choose
			// one for death date.
			addToWhereParemters(whereParameters,
					"deathdate @> '{\"system\": \"http://loinc.org\", \"code\": \"81956-5\"}'::jsonb");

			whereParameters
					.add("component_codings @> '{\"system\": \"http://loinc.org\", \"code\": \"80616-6\"}'::jsonb");

			// where value of input dates.
			for (DateParam dateParam : thePronouncedDeathDate.getValuesAsQueryTokens()) {
				whereParameters.add(constructDateWhereParameter(dateParam, "component", "valueDateTime"));
			}

		}

		if (theDeathDate != null) {
			fromStatement = constructFromStatementPath(fromStatement, "sections", "comp.resource->'section'");
			fromStatement = constructFromStatementPath(fromStatement, "entries", "sections->'entry'");
			fromStatement += " join observation o on entries->>'reference' = concat('Observation/', o.id)";
			fromStatement = constructFromStatementPath(fromStatement, "codings", "sections->'code'->'coding'");
			fromStatement = constructFromStatementPath(fromStatement, "deathdate", "o.resource->'code'->'coding'");

			// We only want a section for circumstance where we keep death location
			// reference.
			whereParameters.add(
					"codings @> '{\"code\": \"jurisdiction\", \"system\": \"" + MdiProfileUtil.CS_MDI_CODES + "\"}'");

			// we want Death Date observation. So, check the code of Observations and choose
			// one for death date.
			addToWhereParemters(whereParameters,
					"deathdate @> '{\"system\": \"http://loinc.org\", \"code\": \"81956-5\"}'::jsonb");

			// where value of input dates.
			for (DateParam dateParam : theDeathDate.getValuesAsQueryTokens()) {
				whereParameters.add(constructDateWhereParameter(dateParam, "o", "valueDateTime"));
			}
		}

		if (theOrTypes != null) {
			fromStatement = constructFromStatementPath(fromStatement, "types", "comp.resource->'type'->'coding'");
			String where = constructTypesWhereParameter(theOrTypes);
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

		if (theTrackingNumber != null) {
			fromStatement = constructFromStatementPath(fromStatement, "extensions", "comp.resource->'extension'");

			String where = "";
			for (TokenParam trackingNumberToken : theTrackingNumber.getValuesAsQueryTokens()) {
				String system = trackingNumberToken.getSystem();
				String value = trackingNumberToken.getValue();
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

		// String subjectResource =
		// subjectReference.getReferenceElement().getResourceType();
		// if (subjectResource != null && !subjectResource.contentEquals("Patient")) {
		// detailCode.setText("Subject (" + subjectResource + ") must be Patient");
		// outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
		// throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		// }
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
			if (reference.getReferenceElement().getResourceType() != null
					&& !reference.getReferenceElement().getResourceType().isBlank()
					&& !addedResource.contains(referenceId)) {
				resource = (Resource) client.read()
						.resource(reference.getReferenceElement().getResourceType())
						.withId(reference.getReferenceElement().getIdPart()).encodedJson()
						.execute();

				if (resource == null || resource.isEmpty()) {
					OperationOutcome outcome = new OperationOutcome();

					outcome.addIssue().setSeverity(IssueSeverity.ERROR)
							.setDetails((new CodeableConcept())
									.setText("resource (" + reference.getReferenceElement().getValue()
											+ ") in Composition section not found"));
					throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
				}

				bundleEntries.add(addToSectAndEntryofDoc(composition, referenceId, resource, addToSection));
				addedResource.add(referenceId);

				if (resource instanceof Practitioner && !addedPractitioner.contains(referenceId)) {
					addedPractitioner.add(resource.getIdElement().getIdPart());
				}
			}
		}

		return resource;
	}

	// private void setupClientForAuth(IGenericClient client) {
	// String authBasic = System.getenv("AUTH_BASIC");
	// String authBearer = System.getenv("AUTH_BEARER");
	// if (authBasic != null && !authBasic.isEmpty()) {
	// String[] auth = authBasic.split(":");
	// if (auth.length == 2) {
	// client.registerInterceptor(new BasicAuthInterceptor(auth[0], auth[1]));
	// }
	// } else if (authBearer != null && !authBearer.isEmpty()) {
	// client.registerInterceptor(new BearerTokenAuthInterceptor(authBearer));
	// }
	// }

	@Search(queryName = CompositionResourceProvider.NQ_EVENT_DETAIL)
	public IBundleProvider searchByNamedQuery(
			@RequiredParam(name = "case-id") TokenAndListParam theCaseIds,
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

	DateParam getDateParam(String date) {
		if (date == null || date.isEmpty())
			return null;

		// check first two characters for prefix.
		ParamPrefixEnum prefix = null;

		if (date.startsWith("eq")) {
			prefix = ParamPrefixEnum.EQUAL;
			date = date.substring(2);
		} else if (date.startsWith("lt")) {
			prefix = ParamPrefixEnum.LESSTHAN;
			date = date.substring(2);
		} else if (date.startsWith("gt")) {
			prefix = ParamPrefixEnum.GREATERTHAN;
			date = date.substring(2);
		} else if (date.startsWith("le")) {
			prefix = ParamPrefixEnum.LESSTHAN_OR_EQUALS;
			date = date.substring(2);
		} else if (date.startsWith("ge")) {
			prefix = ParamPrefixEnum.GREATERTHAN_OR_EQUALS;
			date = date.substring(2);
		} else if (date.startsWith("sa")) {
			prefix = ParamPrefixEnum.STARTS_AFTER;
			date = date.substring(2);
		} else if (date.startsWith("eb")) {
			prefix = ParamPrefixEnum.ENDS_BEFORE;
			date = date.substring(2);
		} else if (date.startsWith("ap")) {
			prefix = ParamPrefixEnum.APPROXIMATE;
			date = date.substring(2);
		}

		DateParam dateParam = new DateParam(prefix, date);

		return dateParam;
	}

	private IQuery<IBaseBundle> queryForDates(IQuery<IBaseBundle> query, DateOrListParam dateRange) {
		List<DateParam> dates = dateRange.getValuesAsQueryTokens();
		boolean shouldQuery = false;
		for (DateParam date : dates) {
			logger.debug("Date Received: " + date.getValueAsString());
			ParamPrefixEnum prefix = date.getPrefix();
			if (prefix == null) {
				query = query
						.and((new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
								.exactly().day(date.getValue()));
			} else {
				if (ParamPrefixEnum.GREATERTHAN_OR_EQUALS == prefix) {
					query = query.and(
							(new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
									.afterOrEquals().day(date.getValue()));
				} else if (ParamPrefixEnum.GREATERTHAN == prefix) {
					query = query.and(
							(new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
									.after().day(date.getValue()));
				} else if (ParamPrefixEnum.LESSTHAN_OR_EQUALS == prefix) {
					query = query.and(
							(new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
									.beforeOrEquals().day(date.getValue()));
				} else if (ParamPrefixEnum.LESSTHAN == prefix) {
					query = query.and(
							(new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
									.before().day(date.getValue()));
				} else {
					query = query.and(
							(new ca.uhn.fhir.rest.gclient.DateClientParam(CompositionResourceProvider.SP_DEATH_DATE))
									.exactly().day(date.getValue()));
				}
			}

			shouldQuery = true;
		}

		if (shouldQuery) {
			return query;
		} else {
			return null;
		}
	}

	// private String toTokenParamString(String token) {
	// int barIndex = ParameterUtil.nonEscapedIndexOf(token, '|');
	// String system = null;
	// String value = null;
	// if (barIndex != -1) {
	// system = token.substring(0, barIndex);
	// value = ParameterUtil.unescape(token.substring(barIndex + 1));
	// } else {
	// value = ParameterUtil.unescape(token);
	// }

	// Identifier identifier = new BaseIdentifierDt();
	// String system = token.getSystem();
	// String value = StringUtils.defaultString(token.getValue());
	// if (StringUtils.isNotBlank(system)) {
	// return system + "|" + value;
	// } else if (system == null) {
	// return value;
	// } else {
	// return "|" + value;
	// }
	// }

	// @Operation(name = "$document", idempotent = true, bundleType =
	// BundleTypeEnum.DOCUMENT)
	// public Bundle generateMdiDocumentOperation(RequestDetails theRequestDetails,
	// @IdParam IdType theCompositionId,
	// @OperationParam(name = "id") UriOrListParam theIds,
	// @OperationParam(name = "persist") BooleanType thePersist,
	// @OperationParam(name = "graph") UriParam theGraph) {

	// return generateDocumentOperation(theRequestDetails, theCompositionId, null,
	// thePersist, theGraph);
	// }

	@Operation(name = "$document", idempotent = true)
	public Bundle generateMdiDocumentOperation(RequestDetails theRequestDetails,
			@IdParam IdType theId,
			@OperationParam(name = "persist") BooleanType thePersist,
			@OperationParam(name = "graph") UriParam theGraph) {

		return generateDocumentOperation(theRequestDetails, theId, null, thePersist, theGraph);
	}

	@Operation(name = "$document", idempotent = true, bundleType = BundleTypeEnum.SEARCHSET)
	public IBundleProvider generateMdiDocumentOperation(RequestDetails theRequestDetails,
			@OperationParam(name = "persist") BooleanType thePersist,
			@OperationParam(name = "graph") UriParam theGraph,
			@OperationParam(name = Composition.SP_PATIENT) List<ParametersParameterComponent> thePatients,
			@OperationParam(name = CompositionResourceProvider.SP_TRACKING_NUMBER) StringOrListParam theTrackingNumber,
			@OperationParam(name = CompositionResourceProvider.SP_DEATH_LOCATION) StringOrListParam theDeathLocations,
			@OperationParam(name = CompositionResourceProvider.SP_DEATH_DATE, max = 2) DateOrListParam theDeathDate,
			@OperationParam(name = CompositionResourceProvider.SP_DEATH_DATE_PRONOUNCED, max = 2) DateOrListParam thePronouncedDeathDate,
			@OperationParam(name = CompositionResourceProvider.SP_MANNER_OF_DEATH) StringOrListParam theMannerOfDeath) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = getTableName() + " comp";

		// Set up join statements.
		if (thePatients != null) {
			// join patient and composition subject tables
			fromStatement += " join patient p on comp.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";
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

		if (theDeathLocations != null) {
			fromStatement = constructFromStatementPath(fromStatement, "sections", "comp.resource->'section'");
			fromStatement = constructFromStatementPath(fromStatement, "entries", "sections->'entry'");
			fromStatement += " join location l on entries->>'reference' = concat('Location/', l.id)";
			fromStatement = constructFromStatementPath(fromStatement, "codings", "sections->'code'->'coding'");

			// We only want a section for circumstance where we keep death location
			// reference.
			whereParameters.add(
					"codings @> '{\"code\": \"circumstances\", \"system\": \"" + MdiProfileUtil.CS_MDI_CODES + "\"}'");

			String districtOrWhere = null;
			for (StringParam theDeathLocation : theDeathLocations.getValuesAsQueryTokens()) {
				String districtOrWhere_ = "lower(l.resource->'address'->>'city') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				districtOrWhere_ += " or lower(l.resource->'address'->>'state') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				districtOrWhere_ += " or lower(l.resource->'address'->>'district') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				districtOrWhere_ += " or lower(l.resource->'address'->>'postalCode') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				districtOrWhere_ += " or lower(l.resource->'address'->>'country') like lower('%"
						+ theDeathLocation.getValue() + "%')";
				if (districtOrWhere == null || districtOrWhere.isEmpty()) {
					districtOrWhere = districtOrWhere_;
				} else {
					districtOrWhere += " or " + districtOrWhere_;
				}
			}

			if (districtOrWhere != null && !districtOrWhere.isBlank()) {
				whereParameters.add(districtOrWhere);
			}
		}

		// Death Date Pronounced
		if (thePronouncedDeathDate != null) {
			fromStatement = constructFromStatementPath(fromStatement, "sections", "comp.resource->'section'");
			fromStatement = constructFromStatementPath(fromStatement, "entries", "sections->'entry'");
			fromStatement += " join observation o on entries->>'reference' = concat('Observation/', o.id)";
			fromStatement = constructFromStatementPath(fromStatement, "codings", "sections->'code'->'coding'");
			fromStatement += ", jsonb_array_elements(o.resource->'component') component, jsonb_array_elements(component->'code'->'coding') component_codings";
			fromStatement = constructFromStatementPath(fromStatement, "deathdate", "o.resource->'code'->'coding'");

			// We only want a section for jurisdiction where we keep death date reference.
			whereParameters.add(
					"codings @> '{\"code\": \"jurisdiction\", \"system\": \"" + MdiProfileUtil.CS_MDI_CODES + "\"}'");

			// we want Death Date observation. So, check the code of Observations and choose
			// one for death date.
			addToWhereParemters(whereParameters,
					"deathdate @> '{\"system\": \"http://loinc.org\", \"code\": \"81956-5\"}'::jsonb");

			whereParameters
					.add("component_codings @> '{\"system\": \"http://loinc.org\", \"code\": \"80616-6\"}'::jsonb");

			// where value of input dates.
			for (DateParam dateParam : thePronouncedDeathDate.getValuesAsQueryTokens()) {
				whereParameters.add(constructDateWhereParameter(dateParam, "component", "valueDateTime"));
			}

		}

		// Death Date
		if (theDeathDate != null) {
			fromStatement = constructFromStatementPath(fromStatement, "sections", "comp.resource->'section'");
			fromStatement = constructFromStatementPath(fromStatement, "entries", "sections->'entry'");
			fromStatement += " join observation o on entries->>'reference' = concat('Observation/', o.id)";
			fromStatement = constructFromStatementPath(fromStatement, "codings", "sections->'code'->'coding'");
			fromStatement = constructFromStatementPath(fromStatement, "deathdate", "o.resource->'code'->'coding'");

			// We only want a section for jurisdiction where we keep death date reference.
			whereParameters.add(
					"codings @> '{\"code\": \"jurisdiction\", \"system\": \"" + MdiProfileUtil.CS_MDI_CODES + "\"}'");

			// we want Death Date observation. So, check the code of Observations and choose
			// one for death date.
			addToWhereParemters(whereParameters,
					"deathdate @> '{\"system\": \"http://loinc.org\", \"code\": \"81956-5\"}'::jsonb");

			// where value of input dates.
			for (DateParam dateParam : theDeathDate.getValuesAsQueryTokens()) {
				whereParameters.add(constructDateWhereParameter(dateParam, "o", "valueDateTime"));
			}
		}

		// Manner of Death
		if (theMannerOfDeath != null) {
			fromStatement = constructFromStatementPath(fromStatement, "sections", "comp.resource->'section'");
			fromStatement = constructFromStatementPath(fromStatement, "entries", "sections->'entry'");
			fromStatement += " join observation o_mod on entries->>'reference' = concat('Observation/', o_mod.id)";
			fromStatement = constructFromStatementPath(fromStatement, "codings", "sections->'code'->'coding'");
			fromStatement = constructFromStatementPath(fromStatement, "mannerOfDeath",
					"o_mod.resource->'code'->'coding'");
			fromStatement = constructFromStatementPath(fromStatement, "mannerOfDeathValue",
					"o_mod.resource->'valueCodeableConcept'->'coding'");

			// We only want a section for cause-manner where we keep manner of death
			// observation reference.
			whereParameters.add(
					"codings @> '{\"code\": \"cause-manner\", \"system\": \"" + MdiProfileUtil.CS_MDI_CODES + "\"}'");

			// we want manner of death observation. So, check the code of Observations and
			// choose one for manner of death.
			addToWhereParemters(whereParameters,
					"mannerOfDeath @> '{\"system\": \"http://loinc.org\", \"code\": \"69449-7\"}'::jsonb");

			String wheres = null;
			for (StringParam tokenParam : theMannerOfDeath.getValuesAsQueryTokens()) {
				String token = tokenParam.getValue();
				int barIndex = ParameterUtil.nonEscapedIndexOf(token, '|');
				String system = null;
				String code = null;
				if (barIndex != -1) {
					system = token.substring(0, barIndex);
					code = ParameterUtil.unescape(token.substring(barIndex + 1));
				} else {
					code = ParameterUtil.unescape(token);
				}

				String whereItem;
				if (system == null || system.isBlank()) {
					whereItem = "mannerOfDeathValue @> '{\"code\": \"" + code + "\"}'::jsonb";
				} else if (code == null || code.isBlank()) {
					whereItem = "mannerOfDeathValue @> '{\"system\": \"" + system + "\"}'::jsonb";
				} else {
					whereItem = "mannerOfDeathValue @> '{\"system\": \"" + system + "\", \"code\": \"" + code
							+ "\"}'::jsonb";
				}

				if (wheres == null) {
					wheres = whereItem;
				} else {
					wheres += " or " + whereItem;
				}
			}

			whereParameters.add(wheres);
		}

		fromStatement = constructFromStatementPath(fromStatement, "typeCodings", "comp.resource->'type'->'coding'");
		String whereMdiDocument = "typeCodings @> '{\"system\": \"" + MdiProfileUtil.MDI_EDRS_DC.getSystem()
				+ "\", \"code\": \"" + MdiProfileUtil.MDI_EDRS_DC.getCode() + "\"}'::jsonb";
		whereParameters.add(whereMdiDocument);

		String whereStatement = constructWhereStatement(whereParameters, null);

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT comp.resource as resource FROM " + fromStatement + whereStatement;

		logger.debug("query count:" + queryCount + "\nquery:" + query);

		MyDocumentBundle myDocumentBundleProvider = new MyDocumentBundle(query, theRequestDetails, null, null);
		myDocumentBundleProvider.setTotalSize(getTotalSize(queryCount));
		myDocumentBundleProvider.setPreferredPageSize(preferredPageSize);

		return myDocumentBundleProvider;
	}

	private Bundle addMessageWrapper(IGenericClient client, Bundle compositionBundle) {
		// the bundle inside of the message Bundle.
		String focusId = BundleResourceProvider.getType() + "/" + compositionBundle.getIdElement().getIdPart();
		Bundle messageHeaders = client.search().forResource(MessageHeader.class)
				.where(MessageHeader.FOCUS.hasId(focusId)).returnBundle(Bundle.class).execute();

		Bundle retMessageBundle = new Bundle();
		MessageHeader messageHeader;
		Bundle originalMessageBundle = null;
		if (messageHeaders != null && !messageHeaders.isEmpty() && messageHeaders.getTotal() > 0) {
			// We may have multiple messageheaders that focus on the same diagnosticreport.
			// We choose the first one.
			messageHeader = (MessageHeader) messageHeaders.getEntryFirstRep().getResource();

			// Now, we find a bundle message that has this messageheader.
			String mhId = MessageHeaderResourceProvider.getType() + "/" + messageHeader.getIdElement().getIdPart();
			Bundle respMessageBundle = client.search().forResource(Bundle.class)
					.where(Bundle.MESSAGE.hasId(mhId)).returnBundle(Bundle.class).execute();
			if (respMessageBundle != null) {
				// We have the search bundle that contains message bundle. Since we are
				// searching
				// one message bundle, just send the first one.
				if (respMessageBundle.getTotal() > 0) {
					originalMessageBundle = (Bundle) respMessageBundle.getEntryFirstRep().getResource();
				}
			}
		} else {
			// Ceate MessageHeader
			UrlType endpoint = new UrlType(OperationUtil.myHostUrl());
			MessageSourceComponent source = new MessageSourceComponent(endpoint);
			messageHeader = new MessageHeader(MdiProfileUtil.DCR_REPORT_EVENT, source);
			messageHeader.setId(new IdType(MessageHeaderResourceProvider.getType(), UUID.randomUUID().toString()));
			messageHeader.addFocus(new Reference(focusId));
		}

		// Add profile to Message Bundle
		retMessageBundle.getMeta()
				.addProfile("http://hl7.org/fhir/us/mdi/StructureDefinition/Bundle-message-death-certificate-review");

		// Create a message bundle and add to messageheader to the entry
		String messageHeaderLocalUrl = MessageHeaderResourceProvider.getType() + "/"
				+ messageHeader.getIdElement().getIdPart();
		BundleEntryComponent bundleEntryComponent = new BundleEntryComponent().setFullUrl(messageHeaderLocalUrl)
				.setResource(messageHeader);
		retMessageBundle.addEntry(bundleEntryComponent);

		// fill out other required fields
		retMessageBundle.setType(BundleType.MESSAGE);

		// Add diagnosticreport to Bundle.entry
		String dcrReportLocalUrl = BundleResourceProvider.getType() + "/"
				+ compositionBundle.getIdElement().getIdPart();
		bundleEntryComponent = new BundleEntryComponent().setFullUrl(dcrReportLocalUrl).setResource(compositionBundle);
		retMessageBundle.addEntry(bundleEntryComponent);

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

	// private Bundle addMessageWrapperToBundleDocument(boolean addMessageWrapper,
	// RequestDetails theRequestDetails, IdType theCompositionId,
	// UriOrListParam theIds,
	// BooleanType thePersist,
	// UriParam theGraph,
	// List<ParametersParameterComponent> thePatients,
	// StringOrListParam theTrackingNumber,
	// StringOrListParam theDeathLocations,
	// DateOrListParam theDeathDatePronounced,
	// DateOrListParam theProfileDeathDateRange,
	// TokenOrListParam theOrTypes) {

	// String myFhirServerBase = theRequestDetails.getFhirServerBase();
	// IGenericClient client =
	// getFhirContext().newRestfulGenericClient(myFhirServerBase);
	// OperationUtil.setupClientForAuth(client);

	// int totalSize = 0;

	// Bundle retBundle = new Bundle();

	// boolean saveIt = false;
	// if (thePersist != null && !thePersist.isEmpty() &&
	// thePersist.getValue().booleanValue()) {
	// saveIt = true;
	// }

	// // if (theCompositionId != null) {
	// // return generateDocumentOperation(theRequestDetails, theCompositionId,
	// null, thePersist, theGraph);

	// // // return client
	// // // .operation()
	// // // .onInstance(theCompositionId)
	// // // .named("")
	// // // .withParameter(Parameters.class, "persist", thePersist)
	// // // .useHttpGet()
	// // // .returnResourceType(Bundle.class)
	// // // .execute();
	// // }

	// // We construct where statement...
	// retBundle.setType(BundleType.SEARCHSET);
	// retBundle.setId(UUID.randomUUID().toString());
	// retBundle.setTotal(totalSize);
	// BundleLinkComponent bundleLinkComponent = new BundleLinkComponent(new
	// StringType("self"), new UriType (theRequestDetails.getCompleteUrl()));
	// retBundle.addLink(bundleLinkComponent);

	// if (theIds != null) {
	// // Composition ID tokens. This is to retrieve documents for the IDs.
	// // This will ignore other search parameters.
	// for (UriParam theId: theIds.getValuesAsQueryTokens()) {
	// // String id = theId.getValue();
	// Bundle compositionBundle = generateDocumentOperation(theRequestDetails, null,
	// theId, thePersist, theGraph);

	// BundleEntryComponent entryComponent = new BundleEntryComponent();
	// if (addMessageWrapper) {
	// Bundle retMessageBundle = addMessageWrapper(client, compositionBundle);

	// entryComponent.setFullUrl(retMessageBundle.getId());
	// entryComponent.setResource(retMessageBundle);
	// } else {
	// // client
	// // .operation().onInstance(new IdType("Composition", id))
	// // .named("")
	// // .withNoParameters(Parameters.class)
	// // .returnResourceType(Bundle.class)
	// // .execute();

	// entryComponent.setFullUrl(compositionBundle.getId());
	// entryComponent.setResource(compositionBundle);
	// }

	// retBundle.addEntry(entryComponent);
	// totalSize++;

	// }

	// retBundle.setTotal(totalSize);

	// if (saveIt) {
	// client.create().resource(retBundle).encodedJson().prettyPrint().execute();
	// }

	// return retBundle;
	// }

	// // boolean shouldQuery = true;

	// Bundle compositionsBundle = null;
	// IQuery<IBaseBundle> query = client.search().forResource(Composition.class);

	// addTokenToIdentifierQuery(query, TRACKING_NUMBER, theTrackingNumber);

	// if (theOrTypes != null) {
	// List<TokenParam> types = theOrTypes.getValuesAsQueryTokens();
	// // ICriterion<TokenClientParam> subQuery = null;
	// List<Coding> codings = new ArrayList<Coding>();
	// for (TokenParam type : types) {
	// String system = type.getSystem();
	// String value = type.getValue();
	// Coding newCoding = new Coding();
	// newCoding.setSystem(system);
	// newCoding.setCode(value);
	// codings.add(newCoding);
	// }

	// query = query.and(Composition.TYPE.exactly().codings(codings.toArray(new
	// Coding[0])));
	// }

	// if (thePatients != null) {
	// for (ParametersParameterComponent thePatient : thePatients) {
	// for (ParametersParameterComponent patientParam : thePatient.getPart()) {
	// if (Patient.SP_FAMILY.equals(patientParam.getName())) {
	// StringType theFamilies = (StringType) patientParam.getValue();
	// if (theFamilies != null && !theFamilies.isEmpty()) {
	// String[] familyStrings = theFamilies.asStringValue().split(",");
	// List<String> familySearchParams = new ArrayList<String>();
	// for (String family : familyStrings) {
	// familySearchParams.add(family);
	// }
	// query =
	// query.and(Composition.PATIENT.hasChainedProperty(Patient.FAMILY.matches().values(familySearchParams)));
	// }
	// } else if (Patient.SP_GIVEN.equals(patientParam.getName())) {
	// StringType theGivens = (StringType) patientParam.getValue();
	// if (theGivens != null && !theGivens.isEmpty()) {
	// String[] givenStrings = theGivens.asStringValue().split(",");
	// List<String> givenSearchParams = new ArrayList<String>();
	// for (String given : givenStrings) {
	// givenSearchParams.add(given);
	// }
	// query =
	// query.and(Composition.PATIENT.hasChainedProperty(Patient.GIVEN.matches().values(givenSearchParams)));
	// }
	// } else if (Patient.SP_GENDER.equals(patientParam.getName())) {
	// StringType theGenders = (StringType) patientParam.getValue();
	// if (theGenders != null && !theGenders.isEmpty()) {
	// String[] genderStrings = theGenders.asStringValue().split(",");
	// List<String> genderSearchParams = new ArrayList<String>();
	// for (String gender : genderStrings) {
	// genderSearchParams.add(gender);
	// }
	// query =
	// query.and(Composition.PATIENT.hasChainedProperty(Patient.GENDER.exactly().codes(genderSearchParams)));
	// }
	// } else if (Patient.SP_BIRTHDATE.equals(patientParam.getName())) {
	// StringType birthDate = (StringType) patientParam.getValue();
	// DateParam date = getDateParam(birthDate.asStringValue());
	// ParamPrefixEnum prefix = date.getPrefix();
	// if (prefix == null) {
	// query =
	// query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.exactly().day(date.getValue())));
	// } else {
	// if (ParamPrefixEnum.GREATERTHAN_OR_EQUALS == prefix) {
	// query =
	// query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.afterOrEquals().day(date.getValue())));
	// } else if (ParamPrefixEnum.GREATERTHAN == prefix) {
	// query =
	// query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.after().day(date.getValue())));
	// } else if (ParamPrefixEnum.LESSTHAN_OR_EQUALS == prefix) {
	// query =
	// query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.beforeOrEquals().day(date.getValue())));
	// } else if (ParamPrefixEnum.LESSTHAN == prefix) {
	// query =
	// query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.before().day(date.getValue())));
	// } else {
	// query =
	// query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.exactly().day(date.getValue())));
	// }
	// }
	// }
	// }
	// }
	// }

	// // Death Location is ONLY referenced from Death Date Observation.
	// // Thus, we need to search Death Date and Location at the same time.
	// List<String> deathLocationSearchParams = new ArrayList<String>();
	// if (theDeathLocations != null) {
	// for (StringParam theDeathLocation :
	// theDeathLocations.getValuesAsQueryTokens()) {
	// deathLocationSearchParams.add(theDeathLocation.getValue());
	// }
	// query = query.and(
	// (new
	// ca.uhn.fhir.rest.gclient.StringClientParam(CompositionResourceProvider.SP_DEATH_LOCATION))
	// .matches()
	// .values(deathLocationSearchParams)
	// );
	// }

	// // Death Date Pronounced
	// if (theDeathDatePronounced != null) {
	// IQuery<IBaseBundle> queryDates = queryForDates(query,
	// theDeathDatePronounced);
	// if (queryDates != null) {
	// query = queryDates;
	// }
	// }

	// // Death Date Presumed
	// // if (theDeathDatePresumed != null) {
	// // IQuery<IBaseBundle> queryDates = queryForDates(query,
	// theDeathDatePresumed);
	// // if (queryDates != null) {
	// // query = queryDates;
	// // shouldQuery = true;
	// // }
	// // }

	// // Death Date
	// if (theProfileDeathDateRange != null) {
	// IQuery<IBaseBundle> queryDates = queryForDates(query,
	// theProfileDeathDateRange);
	// if (queryDates != null) {
	// query = queryDates;
	// }
	// }

	// compositionsBundle = query.returnBundle(Bundle.class).execute();

	// if (compositionsBundle != null && !compositionsBundle.isEmpty()) {
	// List<BundleEntryComponent> entries = compositionsBundle.getEntry();
	// for (BundleEntryComponent entry : entries) {
	// String compositionId = entry.getResource().getIdElement().getIdPart();

	// Bundle documentBundle = generateDocumentOperation(theRequestDetails, new
	// IdType(BundleResourceProvider.getType(), compositionId), null, thePersist,
	// theGraph);

	// BundleEntryComponent entryComponent = new BundleEntryComponent();
	// if (addMessageWrapper) {
	// Bundle retMessageBundle = addMessageWrapper(client, documentBundle);

	// entryComponent.setFullUrl(retMessageBundle.getId());
	// entryComponent.setResource(retMessageBundle);
	// } else {
	// // client
	// // .operation().onInstance(new IdType("Composition", compositionId))
	// // .named("$document")
	// // .withNoParameters(Parameters.class)
	// // .returnResourceType(Bundle.class)
	// // .execute();
	// entryComponent.setFullUrl(documentBundle.getId());
	// entryComponent.setResource(documentBundle);
	// }
	// retBundle.addEntry(entryComponent);

	// totalSize++;
	// }

	// if (saveIt) {
	// client.create().resource(retBundle).encodedJson().prettyPrint().execute();
	// }

	// retBundle.setTotal(totalSize);
	// } else {
	// throwSimulatedOO("Invalid null bundle received from server. Contact
	// developer.");
	// }

	// return retBundle;
	// }

	@Operation(name = "$dcr-message", idempotent = true)
	public Bundle generateDcrMessageReadOperation(RequestDetails theRequestDetails,
			@IdParam IdType theId,
			@OperationParam(name = "persist") BooleanType thePersist,
			@OperationParam(name = "graph") UriParam theGraph) {

		Bundle dcrDocumentBundle = generateDocumentOperation(theRequestDetails, theId, null, thePersist, theGraph);
		if (dcrDocumentBundle == null || dcrDocumentBundle.isEmpty()) {
			ThrowFHIRExceptions.unprocessableEntityException(
					"$dcr-message operation failed to generate dcr document bundle for id = " + theId.asStringValue()
							+ ".");
		}

		// DCR Message wraps the DCR document bundle in the Message Bundle.
		String myFhirServerBase = theRequestDetails.getFhirServerBase();
		IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
		OperationUtil.setupClientForAuth(client);

		return addMessageWrapper(client, dcrDocumentBundle);
	}

	@Operation(name = "$dcr-message", idempotent = true, bundleType = BundleTypeEnum.SEARCHSET)
	public IBundleProvider generateDcrMessageOperation(RequestDetails theRequestDetails,
			@OperationParam(name = "persist") BooleanType thePersist,
			@OperationParam(name = "graph") UriParam theGraph,
			@OperationParam(name = Composition.SP_PATIENT) List<ParametersParameterComponent> thePatients,
			@OperationParam(name = CompositionResourceProvider.SP_TRACKING_NUMBER) StringOrListParam theTrackingNumber) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = getTableName() + " comp";

		// Set up join statements.
		if (thePatients != null) {
			// join patient and composition subject tables
			fromStatement += " join patient p on comp.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";
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

		// Composition also has MDI to EDRS document. For DCR document, we only want DCR
		// type. So, add this to wherestatement.
		fromStatement = constructFromStatementPath(fromStatement, "typeCodings", "comp.resource->'type'->'coding'");
		String whereMdiDocument = "typeCodings @> '{\"system\": \"" + MdiProfileUtil.DCR_REPORT.getSystem()
				+ "\", \"code\": \"" + MdiProfileUtil.DCR_REPORT.getCode() + "\"}'::jsonb";
		whereParameters.add(whereMdiDocument);

		String whereStatement = constructWhereStatement(whereParameters, null);

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT comp.resource as resource FROM " + fromStatement + whereStatement;

		logger.debug("query count:" + queryCount + "\nquery:" + query);

		MyDcrMessageBundle myDcrMessageBundle = new MyDcrMessageBundle(query, theRequestDetails, null, null);
		myDcrMessageBundle.setTotalSize(getTotalSize(queryCount));
		myDcrMessageBundle.setPreferredPageSize(preferredPageSize);

		return myDcrMessageBundle;
	}

	public Bundle generateDocumentOperation(RequestDetails theRequestDetails, IdType theCompositionId,
			UriParam theIdUri, BooleanType thePersist, UriParam theGraph) {

		OperationOutcome outcome = new OperationOutcome();

		boolean saveIt = false;
		if (thePersist != null && !thePersist.isEmpty() && thePersist.getValue().booleanValue()) {
			saveIt = true;
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

		String myFhirServerBase = theRequestDetails.getFhirServerBase();
		IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
		OperationUtil.setupClientForAuth(client);

		Bundle retBundle = constructDocumentBundleFromComposition(client, composition);

		if (saveIt) {
			client.create().resource(retBundle).encodedJson().prettyPrint().execute();
		}

		return retBundle;
	}

	private Bundle constructDocumentBundleFromComposition(IGenericClient client, Composition composition) {
		OperationOutcome outcome = new OperationOutcome();

		CodeableConcept myType = composition.getType();

		if (myType.isEmpty()) {
			// We can't generate a document without type.
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("This composition has no type"));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		Coding typeCoding = null;
		String typeSystem = null;
		String typeCode = null;

		String documentType = null;
		for (Coding coding : myType.getCoding()) {
			if (!coding.isEmpty()) {
				typeCoding = coding;
				typeSystem = coding.getSystem();
				typeCode = coding.getCode();
				if (MdiProfileUtil.MDI_EDRS_DC.getSystem().equalsIgnoreCase(typeSystem)
						&& MdiProfileUtil.MDI_EDRS_DC.getCode().equalsIgnoreCase(typeCode)) {
					documentType = "MDI-DOCUMENT";
					break;
				}

				if (MdiProfileUtil.DCR_REPORT.getSystem().equalsIgnoreCase(typeSystem)
						&& MdiProfileUtil.DCR_REPORT.getCode().equalsIgnoreCase(typeCode)) {
					documentType = "DCR-REPORT";
				}
			}
		}

		if (typeCoding == null || typeCoding.isEmpty()) {
			// We must have coding.
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("This composition type has no coding specified"));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		List<BundleEntryComponent> bundleEntries = new ArrayList<BundleEntryComponent>();

		// First entry must be composition.
		BundleEntryComponent bundleEntry = new BundleEntryComponent();
		bundleEntry.setFullUrlElement(composition.getIdElement());
		bundleEntry.setResource(composition);
		bundleEntries.add(bundleEntry);

		String metaProfile = "";
		if ("MDI-DOCUMENT".equalsIgnoreCase(documentType) || "DCR-REPORT".equalsIgnoreCase(documentType)) {
			// This is a death certificate document or dcr report. We need to add full
			// resources in the
			// section entries
			// to the resources.
			if ("MDI-DOCUMENT".equalsIgnoreCase(documentType)) {
				metaProfile = "http://hl7.org/fhir/us/mdi/StructureDefinition/Bundle-document-mdi-and-edrs";
			} else if ("DCR-REPORT".equalsIgnoreCase(documentType)) {
				metaProfile = "http://hl7.org/fhir/us/mdi/StructureDefinition/Bundle-document-mdi-dcr";
			}

			// The composition section is empty. It means that VRDR has never been
			// generated. We generate it here and persist it.
			// There is no order here. But, put patient first for human eye.
			// If composition section is not empty, we honor that and do not add resources
			// related to
			// death certificate. But, we add them to entry.
			boolean addToSection = false;

			List<String> addedResource = new ArrayList<String>();
			List<String> addedPractitioner = new ArrayList<String>();

			// String patientId =
			// composition.getSubject().getReferenceElement().getIdPart();

			// if (!addedResource.contains("Patient/" + patientId)) {
			// Patient patient =
			// client.read().resource(Patient.class).withId(patientId).encodedJson().execute();
			// bundleEntries.add(addToSectAndEntryofDoc(composition, "Patient/" + patientId,
			// patient, addToSection));
			// addedResource.add("Patient/" + patientId);
			// }

			// Add patient reference
			processReference(client, bundleEntries, addedResource, addedPractitioner, composition,
					composition.getSubject(), addToSection);

			// Add encounter
			processReference(client, bundleEntries, addedResource, addedPractitioner, composition,
					composition.getEncounter(), addToSection);

			// Add authors
			for (Reference reference : composition.getAuthor()) {
				processReference(client, bundleEntries, addedResource, addedPractitioner, composition, reference,
						addToSection);
			}

			// Add Attester
			for (CompositionAttesterComponent attester : composition.getAttester()) {
				processReference(client, bundleEntries, addedResource, addedPractitioner, composition,
						attester.getParty(), addToSection);
			}

			// Add custodian
			processReference(client, bundleEntries, addedResource, addedPractitioner, composition,
					composition.getCustodian(), addToSection);

			// Add related to composition
			for (CompositionRelatesToComponent relatedTo : composition.getRelatesTo()) {
				Type target = relatedTo.getTarget();
				if (target != null && target instanceof Reference) {
					processReference(client, bundleEntries, addedResource, addedPractitioner, composition,
							(Reference) target, addToSection);
				}
			}

			// Add event detail
			for (CompositionEventComponent event : composition.getEvent()) {
				for (Reference detail : event.getDetail()) {
					processReference(client, bundleEntries, addedResource, addedPractitioner, composition, detail,
							addToSection);
				}
			}

			for (SectionComponent section : composition.getSection()) {
				for (Reference reference : section.getEntry()) {
					// String referenceId = reference.getReferenceElement().getValue();
					addToSection = false;

					processReference(client, bundleEntries, addedResource, addedPractitioner, composition, reference,
							addToSection);
				}
			}
		} else {
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("This composition type document is not supported."));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		Bundle retBundle = null;
		// grab some from original bundle.

		// lastly check if the original bundle had a signature.
		Bundle origBundle = client.search().forResource(Bundle.class)
				.where(Bundle.COMPOSITION.hasId(
						new IdType(CompositionResourceProvider.getType(), composition.getIdElement().getIdPart())))
				.returnBundle(Bundle.class).execute();
		if (origBundle != null) {
			if (origBundle.getTotal() > 0) {
				// we use the first bundle returned as our returning bundle.
				retBundle = (Bundle) origBundle.getEntryFirstRep().getResource();
			}
		}

		if (retBundle == null) {
			retBundle = new Bundle();
		}

		// This is generate Document operation. Thus, type must be Document.
		retBundle.setType(Bundle.BundleType.DOCUMENT);

		if (!metaProfile.isEmpty()) {
			Meta meta = retBundle.getMeta();
			boolean addProfile = true;
			for (CanonicalType profiles : meta.getProfile()) {
				if (profiles.equals(metaProfile)) {
					addProfile = false;
					break;
				}
			}

			if (addProfile) {
				meta.addProfile(metaProfile);
				retBundle.setMeta(meta);
			}
		}

		retBundle.setEntry(bundleEntries);
		retBundle.setId(UUID.randomUUID().toString());

		Identifier bundleIdentifier = new Identifier();
		List<Extension> trackingNumExts = composition
				.getExtensionsByUrl("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number");
		for (Extension trackingNumExt : trackingNumExts) {
			Identifier trackingNumIdentifier = (Identifier) trackingNumExt.getValue();
			String code = trackingNumIdentifier.getType().getCodingFirstRep().getCode();
			String system = StringUtils.defaultString(trackingNumIdentifier.getSystem());
			String value = StringUtils.defaultString(trackingNumIdentifier.getValue());
			if ("mdi-case-number".equals(code)) {
				if (system == null || system.isBlank()) {
					system = "urn:raven-fhir-server:document:mdi";
				}
				bundleIdentifier.setSystem(system);
				bundleIdentifier.setValue(value);
				break;
			} else if ("edrs-file-number".equals(code)) {
				if (system == null || system.isBlank()) {
					system = "urn:raven-fhir-server:document:edrs";
				}
				bundleIdentifier.setSystem(system);
				bundleIdentifier.setValue(value);
			}
		}

		if (bundleIdentifier.isEmpty()) {
			bundleIdentifier.setSystem("urn:raven-fhir-server:document:autogenerated");
			bundleIdentifier.setValue(UUID.randomUUID().toString());
		}

		retBundle.setIdentifier(bundleIdentifier);

		retBundle.setTimestamp(new Date());

		return retBundle;
	}

	/*
	 * $update-mdi operation API.
	 */
	@Operation(name = "$update-mdi")
	public Parameters updateMdiDocumentOperation(RequestDetails theRequestDetails,
			@OperationParam(name = CompositionResourceProvider.SP_TRACKING_NUMBER, min = 1, max = 1) TokenParam theTrackingNumbers,
			@OperationParam(name = "mdi-document", min = 1, max = 1) Bundle theBundle) {

		OperationOutcome outcome = new OperationOutcome();

		if (theBundle == null) {
			// theBundle cannot be null. Return OperationOutcome.
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("mdi-document is missing"));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		if (theTrackingNumbers == null) {
			// theBundle cannot be null. Return OperationOutcome.
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("tracking-number is missing"));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		String myFhirServerBase = theRequestDetails.getFhirServerBase();
		IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
		OperationUtil.setupClientForAuth(client);

		BundleEntryComponent firstEntry = theBundle.getEntryFirstRep();
		Resource resource = firstEntry.getResource();
		if (resource instanceof Composition) {
			Composition composition = (Composition) resource;
			CodeableConcept myType = composition.getType();

			if (myType.isEmpty()) {
				// We can't generate a document without type.
				outcome.addIssue().setSeverity(IssueSeverity.ERROR)
						.setDetails((new CodeableConcept()).setText("This composition has no type"));
				throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
			}

			Coding typeCoding = null;
			String documentType = null;
			for (Coding coding : myType.getCoding()) {
				if (!coding.isEmpty()) {
					typeCoding = coding;
					String typeSystem = coding.getSystem();
					String typeCode = coding.getCode();
					if ("http://loinc.org".equalsIgnoreCase(typeSystem) && "86807-5".equalsIgnoreCase(typeCode)) {
						documentType = "MDI-DOCUMENT";
					} else if ("http://loinc.org".equalsIgnoreCase(typeSystem)
							&& "11502-2".equalsIgnoreCase(typeCode)) {
						documentType = "LAB-DOCUMENT";
					} else if (ExtensionUtil.extTrackingNumberTypeSystem.equalsIgnoreCase(typeSystem)
							&& "death-certificate-data-review-doc".equalsIgnoreCase(typeCode)) {
						documentType = "DCR-DOCUMENT";
					}
				}
			}

			if (typeCoding == null || typeCoding.isEmpty()) {
				// We must have coding.
				outcome.addIssue().setSeverity(IssueSeverity.ERROR)
						.setDetails((new CodeableConcept()).setText("This composition type has no coding specified"));
				throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
			}

			String identifierSystem = null;
			String identifierValue = null;
			if ("MDI-DOCUMENT".equals(documentType)) {
				// get EDRS case number from the attached composition. Make sure this tracking
				// number is same
				// as the tracking number that we have in the Parameter. If this does not match,
				// this is an error.
				for (Extension ext : composition.getExtension()) {
					if (ExtensionUtil.extTrackingNumberUrl.equals(ext.getUrl())) {
						// Get valueIdentifier of Tracking Numbers Extension.
						Identifier trackingIdentifier = (Identifier) ext.getValue();
						if (trackingIdentifier != null && !trackingIdentifier.isEmpty()) {
							CodeableConcept tType = trackingIdentifier.getType();
							if (tType != null && !tType.isEmpty()) {
								// Search for edrs-file-number and store the identifier number.
								for (Coding code : tType.getCoding()) {
									if (ExtensionUtil.extTrackingNumberTypeSystem.equals(code.getSystem())
											&& ExtensionUtil.edrsFileNumber.getCode().equals(code.getCode())) {
										identifierSystem = trackingIdentifier.getSystem();
										identifierValue = trackingIdentifier.getValue();
										break;
									}
								}
							}
						}
					}

					if (identifierValue != null) {
						break;
					}
				}

				if (identifierValue == null || identifierValue.isBlank()) {
					// It's Ok that this Composition does not have tracking number. We haave this in
					// the Parameter. Write this into the Composition.

					Extension trackingNumberExt = new Extension();
					Identifier edrsTrackingId = new Identifier();
					edrsTrackingId.setType(new CodeableConcept(ExtensionUtil.edrsFileNumber));
					edrsTrackingId.setSystem(theTrackingNumbers.getSystem());
					edrsTrackingId.setValue(theTrackingNumbers.getValue());

					trackingNumberExt.setUrl(ExtensionUtil.extTrackingNumberUrl);
					trackingNumberExt.setValue(edrsTrackingId);

					composition.addExtension(trackingNumberExt);

					// The edrs-file-number does not exist in the update-mdi request bundle. 
					// So, we write it into the bundle with the values in the Parameters.
					identifierSystem = theTrackingNumbers.getSystem();
					identifierValue = theTrackingNumbers.getValue();
				}

				// Now, check tracking number in Parameter and Composition.
				if (!theTrackingNumbers.getSystem().equals(identifierSystem)
						|| !theTrackingNumbers.getValue().equals(identifierValue)) {
					outcome.addIssue().setSeverity(IssueSeverity.ERROR)
							.setDetails((new CodeableConcept())
									.setText("tracking-number does not match with the Tracking Number in the bundle"));
					throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
				}
				
				// Search the composition that has this tracking number.
				Bundle compositionBundle;
				if (identifierSystem != null && !identifierSystem.isEmpty()) {
					compositionBundle = client.search().forResource(Composition.class)
							.and(CompositionResourceProvider.TRACKING_NUMBER
									.exactly()
									.systemAndCode(identifierSystem, identifierValue))
							.returnBundle(Bundle.class).execute();
				} else {
					compositionBundle = client.search().forResource(Composition.class)
							.and(CompositionResourceProvider.TRACKING_NUMBER
									.exactly()
									.code(identifierValue))
							.returnBundle(Bundle.class).execute();
				}

				if (compositionBundle.getTotal() == 0) {
					outcome.addIssue().setSeverity(IssueSeverity.ERROR)
							.setDetails((new CodeableConcept()).setText("MDI and EDRS Document for edrs-file-number (" + identifierSystem + "|"
									+ identifierValue + ") could not be found"));
					throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
				}

			// } else if ("DCR-DOCUMENT".equals(documentType)) {
			// 	// get case number.
			// 	for (Extension ext : composition.getExtension()) {
			// 		if (ExtensionUtil.extTrackingNumberUrl.equals(ext.getUrl())) {
			// 			Identifier trackingIdentifier = (Identifier) ext.getValue();
			// 			CodeableConcept tType = trackingIdentifier.getType();
			// 			if (tType != null && !tType.isEmpty()) {
			// 				for (Coding code : tType.getCoding()) {
			// 					if (ExtensionUtil.extTrackingNumberTypeSystem.equals(code.getSystem())
			// 							&& ExtensionUtil.funeralHomeCaseNumber.getCode().equals(code.getCode())) {
			// 						identifierSystem = trackingIdentifier.getSystem();
			// 						identifierValue = trackingIdentifier.getValue();
			// 					}
			// 				}
			// 			}
			// 		}
			// 	}

			// 	if (identifierValue == null) {
			// 		outcome.addIssue().setSeverity(IssueSeverity.ERROR)
			// 				.setDetails((new CodeableConcept())
			// 						.setText("Tracking number for Funeral Home not found in the bundle"));
			// 		throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
			// 	}

			// 	// Search the composition that has this tracking number.
			// 	Bundle compositionBundle;
			// 	if (identifierSystem != null && !identifierSystem.isEmpty()) {
			// 		compositionBundle = client.search().forResource(Composition.class)
			// 				.and(CompositionResourceProvider.TRACKING_NUMBER
			// 						.exactly()
			// 						.systemAndCode(identifierSystem, identifierValue))
			// 				.returnBundle(Bundle.class).execute();
			// 	} else {
			// 		compositionBundle = client.search().forResource(Composition.class)
			// 				.and(CompositionResourceProvider.TRACKING_NUMBER
			// 						.exactly()
			// 						.code(identifierValue))
			// 				.returnBundle(Bundle.class).execute();
			// 	}
			// 	BundleEntryComponent entry = compositionBundle.getEntryFirstRep();
			// 	if (entry.isEmpty()) {
			// 		outcome.addIssue().setSeverity(IssueSeverity.ERROR)
			// 				.setDetails((new CodeableConcept()).setText("Funeral Home Case Number (" + identifierSystem
			// 						+ "|" + identifierValue + ") is not found"));
			// 		throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
			// 	}
			} else {
				outcome.addIssue().setSeverity(IssueSeverity.ERROR)
						.setDetails((new CodeableConcept()).setText("Only MDI-and-EDRS document is supported."));
				throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
			}
		} else {
			outcome.addIssue().setSeverity(IssueSeverity.ERROR)
					.setDetails((new CodeableConcept()).setText("The first entry MUST be composition"));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		// Now we are good to go. Make the transaction.
		Parameters responseParameter = new Parameters();
		Bundle out = client.transaction().withBundle(theBundle).execute();
		if (out != null && !out.isEmpty()) {
			ParametersParameterComponent parameter = new ParametersParameterComponent();
			parameter.setName("mdi-document");
			parameter.setResource(out);
			responseParameter.addParameter(parameter);
		}

		ParametersParameterComponent warningParam = new ParametersParameterComponent();
		warningParam.setName("warning");

		outcome.addIssue().setSeverity(IssueSeverity.WARNING)
			.setDetails((new CodeableConcept()).setText("This operation is for simple demonstration of update-mdi API. This just replaced entire existing document."));

		warningParam.setResource(outcome);
		responseParameter.addParameter(warningParam);

		return responseParameter;
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

			logger.debug("calling database: " + myQuery);
			try {
				retVal.addAll(getFhirbaseMapping().search(myQuery, getResourceType()));
			} catch (SQLException e) {
				e.printStackTrace();
			}

			return retVal;
		}
	}

	class MyDocumentBundle extends FhirbaseBundleProvider {
		RequestDetails theRequestDetails;

		public MyDocumentBundle(String query, RequestDetails theRequestDetails, Set<Include> theIncludes,
				Set<Include> theReverseIncludes) {
			super(query);
			setPreferredPageSize(preferredPageSize);
			this.theRequestDetails = theRequestDetails;
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			List<IBaseResource> documentBundles = new ArrayList<IBaseResource>();
			List<IBaseResource> retResources = new ArrayList<IBaseResource>();

			String myQuery = query;
			if (toIndex - fromIndex > 0) {
				myQuery += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			logger.debug("Generate documents alling database: " + myQuery);
			try {
				retResources = getFhirbaseMapping().search(myQuery, getResourceType());
			} catch (SQLException e) {
				e.printStackTrace();
			}

			// Make the compositions to Document Bundle.
			String myFhirServerBase = this.theRequestDetails.getFhirServerBase();
			IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
			OperationUtil.setupClientForAuth(client);

			for (IBaseResource composition : retResources) {
				Composition composition_ = (Composition) composition;
				Bundle retBundle = constructDocumentBundleFromComposition(client, composition_);

				documentBundles.add(retBundle);
			}

			return documentBundles;
		}
	}

	class MyDcrMessageBundle extends FhirbaseBundleProvider {
		RequestDetails theRequestDetails;

		public MyDcrMessageBundle(String query, RequestDetails theRequestDetails, Set<Include> theIncludes,
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

			logger.debug("Generate documents alling database: " + myQuery);
			try {
				retResources = getFhirbaseMapping().search(myQuery, getResourceType());
			} catch (SQLException e) {
				e.printStackTrace();
			}

			// Make the compositions to Document Bundle.
			String myFhirServerBase = this.theRequestDetails.getFhirServerBase();
			IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
			OperationUtil.setupClientForAuth(client);

			for (IBaseResource composition : retResources) {
				Composition composition_ = (Composition) composition;
				Bundle retBundle = constructDocumentBundleFromComposition(client, composition_);

				Bundle messageBundle = addMessageWrapper(client, retBundle);
				messageBundles.add(messageBundle);
			}

			return messageBundles;
		}
	}
}
