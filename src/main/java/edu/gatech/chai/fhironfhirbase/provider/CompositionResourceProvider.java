package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
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
import org.hl7.fhir.r4.model.Composition.CompositionAttesterComponent;
import org.hl7.fhir.r4.model.Composition.CompositionEventComponent;
import org.hl7.fhir.r4.model.Composition.CompositionRelatesToComponent;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Practitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.IElement;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.model.api.annotation.SearchParamDefinition;
import ca.uhn.fhir.model.base.composite.BaseIdentifierDt;
import ca.uhn.fhir.model.primitive.StringDt;
import ca.uhn.fhir.model.primitive.UriDt;
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
import ca.uhn.fhir.rest.gclient.TokenClientParam;
import ca.uhn.fhir.rest.param.DateOrListParam;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.DateRangeParam;
import ca.uhn.fhir.rest.param.ParamPrefixEnum;
import ca.uhn.fhir.rest.param.ParameterUtil;
import ca.uhn.fhir.rest.param.ReferenceAndListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
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
import edu.gatech.chai.fhironfhirbase.utilities.OperationUtil;

@Service
@Scope("prototype")
public class CompositionResourceProvider extends BaseResourceProvider {
	private static final Logger logger = LoggerFactory.getLogger(CompositionResourceProvider.class);
	public static final String NQ_EVENT_DETAIL = "event-detail";

	/**
	 * Search parameter: <b>death-date-actual</b>
	 * <p>
	 * Description: <b>Actual Date of Death</b><br>
   	 * Type: <b>date</b><br>
	 * Path: <b>Observation.valueDateTime</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name="death-date-presumed", path="Observation.valueDateTime", description="Actual Date of Death", type="date" )
	public static final String SP_DEATH_DATE_PRESUMED = "death-date-presumed";
	/**
   	 * <b>Fluent Client</b> search parameter constant for <b>death-date-presumed</b>
	 * <p>
	 * Description: <b>Presumed Date of Death</b><br>
   	 * Type: <b>date</b><br>
	 * Path: <b>Observation.valueDateTime</b><br>
	 * </p>
   	 */
	public static final ca.uhn.fhir.rest.gclient.DateClientParam DEATH_DATE_PRESUMED = new ca.uhn.fhir.rest.gclient.DateClientParam(SP_DEATH_DATE_PRESUMED);

	/**
	 * Search parameter: <b>death-date-pronounced</b>
	 * <p>
	 * Description: <b>Actual Date of Pronounced Death</b><br>
   	 * Type: <b>date</b><br>
	 * Path: <b>Observation.component</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name="death-date-pronounced", path="Observation.valueDateTime", description="Date of Pronounced Death", type="date" )
	public static final String SP_DEATH_DATE_PRONOUNCED = "death-date-pronounced";
	/**
   	 * <b>Fluent Client</b> search parameter constant for <b>death-date-pronounced</b>
	 * <p>
	 * Description: <b>Actual Date of Pronounced Death</b><br>
   	 * Type: <b>date</b><br>
	 * Path: <b>Observation.component</b><br>
	 * </p>
   	 */
	public static final ca.uhn.fhir.rest.gclient.DateClientParam DEATH_DATE_PRONOUNCED = new ca.uhn.fhir.rest.gclient.DateClientParam(SP_DEATH_DATE_PRONOUNCED);

	/**
	 * Search parameter: <b>death-date</b>
	 * <p>
	 * Description: <b>Date of Death</b><br>
   	 * Type: <b>date</b><br>
	 * Path: <b>Observation.valueDateTime or Observation.component</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name="death-date", path="Observation.component", description="Date of Death", type="date" )
	public static final String SP_DEATH_DATE = "death-date";
	/**
   	 * <b>Fluent Client</b> search parameter constant for <b>death-date-pronounced</b>
	 * <p>
	 * Description: <b>Date of Death</b><br>
   	 * Type: <b>date</b><br>
	 * Path: <b>Observation.valueDateTime or Observation.component</b><br>
	 * </p>
   	 */
	public static final ca.uhn.fhir.rest.gclient.DateClientParam DEATH_DATE = new ca.uhn.fhir.rest.gclient.DateClientParam(SP_DEATH_DATE);

	/**
	 * Search parameter: <b>death-location</b>
	 * <p>
	 * Description: <b>District of death location</b><br>
   	 * Type: <b>string</b><br>
	 * Path: <b>Composition.death-location</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name="death-location", path="Location.name", description="District of death location", type="string" )
	public static final String SP_DEATH_LOCATION = "death-location";
	/**
   	 * <b>Fluent Client</b> search parameter constant for <b>edrs-file-number</b>
	 * <p>
	 * Description: <b>District of death location</b><br>
   	 * Type: <b>string</b><br>
	 * Path: <b>Composition.death-location</b><br>
	 * </p>
   	 */
	public static final ca.uhn.fhir.rest.gclient.StringClientParam DEATH_LOCATION = new ca.uhn.fhir.rest.gclient.StringClientParam(SP_DEATH_LOCATION);

	/**
	 * Search parameter: <b>edrs-file-number</b>
	 * <p>
	 * Description: <b>A composition extension identifier for edrs-file-number</b><br>
   	 * Type: <b>token</b><br>
	 * Path: <b>Composition.edrs-file-number</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name="edrs-file-number", path="Composition.extension-tracking-numbers", description="Extension Trakcing Number for EDRS file", type="token" )
	public static final String SP_EDRS_FILE_NUMBER = "edrs-file-number";
	/**
   	 * <b>Fluent Client</b> search parameter constant for <b>edrs-file-number</b>
	 * <p>
	 * Description: <b>A composition extension identifier for edrs-file-number</b><br>
   	 * Type: <b>token</b><br>
	 * Path: <b>Composition.edrs-file-number</b><br>
	 * </p>
   	 */
	public static final ca.uhn.fhir.rest.gclient.TokenClientParam EDRS_FILE_NUMBER = new ca.uhn.fhir.rest.gclient.TokenClientParam(SP_EDRS_FILE_NUMBER);

	/**
	 * Search parameter: <b>mdi-case-number</b>
	 * <p>
	 * Description: <b>A composition extension identifier for mdi-case-number</b><br>
   	 * Type: <b>token</b><br>
	 * Path: <b>Composition.mdi-case-number</b><br>
	 * </p>
	 */
	@SearchParamDefinition(name="mdi-case-number", path="Composition.extension-tracking-numbers", description="Extension Trakcing Number for MDI case", type="token" )
	public static final String SP_MDI_CASE_NUMBER = "mdi-case-number";
	/**
   	 * <b>Fluent Client</b> search parameter constant for <b>mdi-case-number</b>
   	 * <p>
	 * Description: <b>A composition extension identifier for mdi-case-number</b><br>
	 * Type: <b>token</b><br>
   	 * Path: <b>Composition.mdi-case-number</b><br>
   	 * </p>
   	 */
  	public static final ca.uhn.fhir.rest.gclient.TokenClientParam MDI_CASE_NUMBER = new ca.uhn.fhir.rest.gclient.TokenClientParam(SP_MDI_CASE_NUMBER);
	

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
			@OptionalParam(name = CompositionResourceProvider.SP_DEATH_DATE_PRESUMED) DateRangeParam theDeathDatePresumed,
			@OptionalParam(name = CompositionResourceProvider.SP_DEATH_DATE_PRONOUNCED) DateRangeParam theDeathDatePronounced,
			@OptionalParam(name = CompositionResourceProvider.SP_DEATH_DATE) DateRangeParam theDeathDate,
			@OptionalParam(name = CompositionResourceProvider.SP_EDRS_FILE_NUMBER) TokenOrListParam theEdrsFileNumber,
			@OptionalParam(name = CompositionResourceProvider.SP_MDI_CASE_NUMBER) TokenOrListParam theMdiCaseNumber,
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

		if (theDeathLocations != null || theDeathDate != null || theDeathDatePronounced != null || theDeathDatePresumed != null) {
			// join observation and composition tables on subject reference
			fromStatement += " join observation o on comp.resource->'subject'->>'reference' = o.resource->'subject'->>'reference'";
		}

		if (theDeathLocations != null) {
			fromStatement += " join location l on o.resource->'extension'->0->>'url' = 'http://hl7.org/fhir/us/vrdr/StructureDefinition/Observation-Location' " 
				+ "and o.resource->'extension'->0->'valueReference'->>'reference' = concat('Location/', l.resource->>'id')";
		}

		if (theDeathDate != null || theDeathDatePronounced != null) {
			// the pronunced death date is available in the component section
			fromStatement += ", jsonb_array_elements(o.resource->'component') component, jsonb_array_elements(component->'code'->'coding') component_codings";
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
					districtOrWhere = "lower(l.resource->'address'->>'district') like lower('%" + theDeathLocation.getValue() + "%')";
				} else {
					districtOrWhere += " or lower(l.resource->'address'->>'district') like lower('%" + theDeathLocation.getValue() + "%')";
				}
			}

			whereParameters.add(districtOrWhere);
		}

		if (theDeathDate != null) {
			// add presumed date to path
			fromStatement = constructFromStatementPath(fromStatement, "deathdate", "o.resource->'code'->'coding'");
			addToWhereParemters(whereParameters, "deathdate @> '{\"system\": \"http://loinc.org\", \"code\": \"81956-5\"}'::jsonb");
			
			// check observation.valueDateTime
			String deathDateWhere = constructDateRangeWhereParameter(theDeathDate, "o", "valueDateTime");

			String componentWhere = "";
			String prouncedDateWhere = constructDateRangeAliasPathWhere(theDeathDate, "component", "valueDateTime");
			if (prouncedDateWhere != null && !prouncedDateWhere.isEmpty()) {
				componentWhere = "(component_codings @> '{\"system\": \"http://loinc.org\", \"code\": \"80616-6\"}'::jsonb and " + prouncedDateWhere + ")";
			}

			if (componentWhere != null && !componentWhere.isEmpty()) {
				if (deathDateWhere != null && !deathDateWhere.isEmpty()) {
					deathDateWhere += " or " + componentWhere;
				} else {
					deathDateWhere = componentWhere;
				}
			}

			if (deathDateWhere != null && !deathDateWhere.isEmpty()) {
				whereParameters.add(deathDateWhere);
			}
		} else {
			// If theDeathDate is selected, then presumed and pronunced both will be examined with 'OR'. Thus, presumed
			// and pronounced will only be examined when deathdate search is not used. 
			if (theDeathDatePresumed != null) {
				// add presumed date to path
				fromStatement = constructFromStatementPath(fromStatement, "deathdate", "o.resource->'code'->'coding'");
				addToWhereParemters(whereParameters, "deathdate @> '{\"system\": \"http://loinc.org\", \"code\": \"81956-5\"}'::jsonb");
				
				// check observation.valueDateTime
				String deathDateWhere = constructDateRangeWhereParameter(theDeathDate, "o", "valueDateTime");
				if (deathDateWhere != null && !deathDateWhere.isEmpty()) {
					whereParameters.add(deathDateWhere);
				}
			}
	
			if (theDeathDatePronounced != null) {
				// check pronounced death tiem component valueDate
				// Add component code in where statement
				whereParameters.add("component_codings @> '{\"system\": \"http://loinc.org\", \"code\": \"80616-6\"}'::jsonb");
				String prouncedDateWhere = constructDateRangeAliasPathWhere(theDeathDate, "component", "valueDateTime");
				if (prouncedDateWhere != null && !prouncedDateWhere.isEmpty()) {
					whereParameters.add(prouncedDateWhere);
				}
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

		if (theEdrsFileNumber != null) {
			fromStatement = constructFromStatementPath(fromStatement, "extensions", "comp.resource->'extension'");

			String where = "";
			for (TokenParam edrsFileNumberToken : theEdrsFileNumber.getValuesAsQueryTokens()) {
				String system = edrsFileNumberToken.getSystem();
				String value = edrsFileNumberToken.getValue();
				String whereItem;
				if (system == null || system.isBlank()) {
					whereItem = "extensions @> '{\"url\": \"http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes\", \"code\":\"" + SP_EDRS_FILE_NUMBER + "\"}]}, \"value\": \"" + value + "\"}}'::jsonb";
				} else if (value == null || value.isBlank()) {
					whereItem = "extensions @> '{\"url\": \"http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes\", \"code\":\"" + SP_EDRS_FILE_NUMBER + "\"}]}, \"system\": \"" + system + "\"}}'::jsonb";
				} else {
					whereItem = "extensions @> '{\"url\": \"http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes\", \"code\":\"" + SP_EDRS_FILE_NUMBER + "\"}]}, \"system\": \"" + system + "\", \"value\": \"" + value + "\"}}'::jsonb";
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

		if (theMdiCaseNumber != null) {
			fromStatement = constructFromStatementPath(fromStatement, "extensions", "comp.resource->'extension'");

			String where = "";
			for (TokenParam mdiCaseNumberToken : theMdiCaseNumber.getValuesAsQueryTokens()) {
				// TokenParam token = new TokenParam(mdiCaseNumberToken.getValue());
				
				// String system = token.getSystem();
				// String value = token.getValue();
				String system = mdiCaseNumberToken.getSystem();
				String value = mdiCaseNumberToken.getValue();
				String whereItem;
				if (system == null || system.isBlank()) {
					whereItem = "extensions @> '{\"url\": \"http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes\", \"code\":\"" + SP_MDI_CASE_NUMBER + "\"}]}, \"value\": \"" + value + "\"}}'::jsonb";
				} else if (value == null || value.isBlank()) { 
					whereItem = "extensions @> '{\"url\": \"http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes\", \"code\":\"" + SP_MDI_CASE_NUMBER + "\"}]}, \"system\": \"" + system + "\"}}'::jsonb";
				} else {
					whereItem = "extensions @> '{\"url\": \"http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number\", \"valueIdentifier\": {\"type\": {\"coding\": [{\"system\": \"http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes\", \"code\":\"" + SP_MDI_CASE_NUMBER + "\"}]}, \"system\": \"" + system + "\", \"value\": \"" + value + "\"}}'::jsonb";
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
	// 	String authBasic = System.getenv("AUTH_BASIC");
	// 	String authBearer = System.getenv("AUTH_BEARER");
	// 	if (authBasic != null && !authBasic.isEmpty()) {
	// 		String[] auth = authBasic.split(":");
	// 		if (auth.length == 2) {
	// 			client.registerInterceptor(new BasicAuthInterceptor(auth[0], auth[1]));
	// 		}
	// 	} else if (authBearer != null && !authBearer.isEmpty()) {
	// 		client.registerInterceptor(new BearerTokenAuthInterceptor(authBearer));
	// 	}
	// }

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

	DateParam getDateParam(String date) {
		if (date == null || date.isEmpty()) return null;

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

	private IQuery<IBaseBundle> queryForDates (IQuery<IBaseBundle> query, DateOrListParam dateRange) {
		List<DateParam> dates = dateRange.getValuesAsQueryTokens();
		boolean shouldQuery = false;
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

		if (shouldQuery) {
			return query;
		} else {
			return null;
		}
	}

	// private String toTokenParamString(String token) {
	// 	int barIndex = ParameterUtil.nonEscapedIndexOf(token, '|');
	// 	String system = null;
	// 	String value = null;
	// 	if (barIndex != -1) {
	// 		system = token.substring(0, barIndex);
	// 		value = ParameterUtil.unescape(token.substring(barIndex + 1));
	// 	} else {
	// 		value = ParameterUtil.unescape(token);
	// 	}

	// 	Identifier identifier = new BaseIdentifierDt();
	// 	String system = token.getSystem();
	// 	String value = StringUtils.defaultString(token.getValue());
	// 	if (StringUtils.isNotBlank(system)) {
	// 		return system + "|" + value;
	// 	} else if (system == null) {
	// 		return value;
	// 	} else {
	// 		return "|" + value;
	// 	}
	// }

	@Operation(name = "$mdi-documents", idempotent = true, bundleType = BundleTypeEnum.SEARCHSET)
	public Bundle generateMdiDocumentOperation(RequestDetails theRequestDetails, 
			@IdParam(optional=true) IdType theCompositionId,
			@OperationParam(name = "id") UriOrListParam theIds, 
			@OperationParam(name = "persist") BooleanType thePersist,
			@OperationParam(name = Composition.SP_PATIENT) List<ParametersParameterComponent> thePatients,
			@OperationParam(name = CompositionResourceProvider.SP_EDRS_FILE_NUMBER) StringOrListParam theEdrsFileNumber,
			@OperationParam(name = CompositionResourceProvider.SP_MDI_CASE_NUMBER) StringOrListParam theMdiCaseNumber,
			@OperationParam(name = CompositionResourceProvider.SP_DEATH_LOCATION) StringOrListParam theDeathLocations,
			@OperationParam(name = CompositionResourceProvider.SP_DEATH_DATE_PRESUMED, max = 2) DateOrListParam theDeathDatePresumed,  
			@OperationParam(name = CompositionResourceProvider.SP_DEATH_DATE_PRONOUNCED, max = 2) DateOrListParam theDeathDatePronounced,
			@OperationParam(name = CompositionResourceProvider.SP_DEATH_DATE, max = 2) DateOrListParam theProfileDeathDateRange) {
				
		String myFhirServerBase = theRequestDetails.getFhirServerBase();
		IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
		OperationUtil.setupClientForAuth(client);

		int totalSize = 0;

		Bundle retBundle = new Bundle();

		boolean saveIt = false;
		if (thePersist != null && !thePersist.isEmpty() && thePersist.getValue().booleanValue()) {
			saveIt = true;
		}

		if (theCompositionId != null) {
			// if we have the composition id, then all search parameters will be ignored.
			if (thePersist == null) {
				thePersist = new BooleanType(false);
			}

			return client
				.operation()
				.onInstance(theCompositionId)
				.named("$document")
				.withParameter(Parameters.class, "persist", thePersist)
				.useHttpGet()
				.returnResourceType(Bundle.class)
				.execute();
		}

		// We construct where statement...
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

			if (saveIt) {
				client.create().resource(retBundle).encodedJson().prettyPrint().execute();
			}
	
			return retBundle;
		}
		
		boolean shouldQuery = false;

		Bundle compositionsBundle = null;
		IQuery<IBaseBundle> query = client.search().forResource(Composition.class);

		if (addTokenToIdentifierQuery(query, EDRS_FILE_NUMBER, theEdrsFileNumber) == true) {
			shouldQuery = true;
		}

 		if (addTokenToIdentifierQuery(query, MDI_CASE_NUMBER, theMdiCaseNumber) == true) {
			shouldQuery = true;
		}

		if (thePatients != null) {
			for (ParametersParameterComponent thePatient : thePatients) {
				for (ParametersParameterComponent patientParam : thePatient.getPart()) {
					if (Patient.SP_FAMILY.equals(patientParam.getName())) {
						StringType theFamilies = (StringType) patientParam.getValue();
						if (theFamilies != null && !theFamilies.isEmpty()) {
							String[] familyStrings = theFamilies.asStringValue().split(",");
							List<String> familySearchParams = new ArrayList<String>();
							for (String family : familyStrings) {
								familySearchParams.add(family);
							}
							query = query.and(Composition.PATIENT.hasChainedProperty(Patient.FAMILY.matches().values(familySearchParams)));
							shouldQuery = true;
						}		
					} else if (Patient.SP_GIVEN.equals(patientParam.getName())) {
						StringType theGivens = (StringType) patientParam.getValue();
						if (theGivens != null && !theGivens.isEmpty()) {
							String[] givenStrings = theGivens.asStringValue().split(",");
							List<String> givenSearchParams = new ArrayList<String>();
							for (String given : givenStrings) {
								givenSearchParams.add(given);
							}
							query = query.and(Composition.PATIENT.hasChainedProperty(Patient.GIVEN.matches().values(givenSearchParams)));
							shouldQuery = true;		
						}
					} else if (Patient.SP_GENDER.equals(patientParam.getName())) {
						StringType theGenders = (StringType) patientParam.getValue();
						if (theGenders != null && !theGenders.isEmpty()) {
							String[] genderStrings = theGenders.asStringValue().split(",");
							List<String> genderSearchParams = new ArrayList<String>();
							for (String gender : genderStrings) {
								genderSearchParams.add(gender);
							}
							query = query.and(Composition.PATIENT.hasChainedProperty(Patient.GENDER.exactly().codes(genderSearchParams)));
							shouldQuery = true;
						}
					} else if (Patient.SP_BIRTHDATE.equals(patientParam.getName())) {
						StringType birthDate = (StringType) patientParam.getValue();
						DateParam date = getDateParam(birthDate.asStringValue());
						ParamPrefixEnum prefix = date.getPrefix();
						if (prefix == null) {
							query = query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.exactly().day(date.getValue())));
						} else {
							if (ParamPrefixEnum.GREATERTHAN_OR_EQUALS == prefix) {
								query = query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.afterOrEquals().day(date.getValue())));
							} else if (ParamPrefixEnum.GREATERTHAN == prefix) {
								query = query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.after().day(date.getValue())));
							} else if (ParamPrefixEnum.LESSTHAN_OR_EQUALS == prefix) {
								query = query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.beforeOrEquals().day(date.getValue())));
							} else if (ParamPrefixEnum.LESSTHAN == prefix) {
								query = query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.before().day(date.getValue())));
							} else {
								query = query.and(Composition.PATIENT.hasChainedProperty(Patient.BIRTHDATE.exactly().day(date.getValue())));
							}						
						}

						shouldQuery = true;
					}
				}
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

		// Death Date Pronounced
		if (theDeathDatePronounced != null) {
			IQuery<IBaseBundle> queryDates = queryForDates(query, theDeathDatePronounced);
			if (queryDates != null) {
				query = queryDates;
				shouldQuery = true;
			}
		}

		// Death Date Presumed
		if (theDeathDatePresumed != null) {
			IQuery<IBaseBundle> queryDates = queryForDates(query, theDeathDatePresumed);
			if (queryDates != null) {
				query = queryDates;
				shouldQuery = true;
			}
		}

		// Death Date 
		if (theProfileDeathDateRange != null) {
			IQuery<IBaseBundle> queryDates = queryForDates(query, theProfileDeathDateRange);
			if (queryDates != null) {
				query = queryDates;
				shouldQuery = true;
			}
		}

		if (shouldQuery) {
			compositionsBundle = query.returnBundle(Bundle.class).execute();
		}

		if (compositionsBundle != null && !compositionsBundle.isEmpty()) {
			List<BundleEntryComponent> entries = compositionsBundle.getEntry();
			for (BundleEntryComponent entry : entries) {
				String compositionId = entry.getResource().getIdElement().getIdPart();

				Bundle documentBundle = client
					.operation().onInstance(new IdType("Composition", compositionId))
					.named("$document")
					.withNoParameters(Parameters.class)
					.returnResourceType(Bundle.class)
					.execute();
				BundleEntryComponent entryComponent = new BundleEntryComponent();
				entryComponent.setFullUrl(documentBundle.getId());
				entryComponent.setResource(documentBundle);
				retBundle.addEntry(entryComponent);

				totalSize++;
			}

			if (saveIt) {
				client.create().resource(retBundle).encodedJson().prettyPrint().execute();
			}
	
			retBundle.setTotal(totalSize);
		} else {
			throwSimulatedOO("No or No Known Parameters are received.");
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
				if ("http://loinc.org".equalsIgnoreCase(typeSystem) && "86807-5".equalsIgnoreCase(typeCode)) {
					documentType = "MDI-DOCUMENT";
					break;
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

		String myFhirServerBase = theRequestDetails.getFhirServerBase();
		IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
		OperationUtil.setupClientForAuth(client);

		String metaProfile = "";
		if ("MDI-DOCUMENT".equalsIgnoreCase(documentType)) {
			// This is a death certificate document. We need to add full resources in the
			// section entries
			// to the resources.
			metaProfile = "http://hl7.org/fhir/us/mdi/StructureDefinition/Bundle-document-mdi-to-edrs";

			// The composition section is empty. It means that VRDR has never been
			// generated. We generate it here and persist it.
			// There is no order here. But, put patient first for human eye.
			// If composition section is not empty, we honor that and do not add resources related to
			// death certificate. But, we add them to entry.
			boolean addToSection = false;
			
			List<String> addedResource = new ArrayList<String>();
			List<String> addedPractitioner = new ArrayList<String>();

			// String patientId = composition.getSubject().getReferenceElement().getIdPart();

			// if (!addedResource.contains("Patient/" + patientId)) {
			// 	Patient patient = client.read().resource(Patient.class).withId(patientId).encodedJson().execute();
			// 	bundleEntries.add(addToSectAndEntryofDoc(composition, "Patient/" + patientId, patient, addToSection));
			// 	addedResource.add("Patient/" + patientId);
			// }

			// Add patient reference
			processReference(client, bundleEntries, addedResource, addedPractitioner, composition, composition.getSubject(), addToSection);

			// Add encounter
			processReference(client, bundleEntries, addedResource, addedPractitioner, composition, composition.getEncounter(), addToSection);

			// Add authors
			for (Reference reference : composition.getAuthor()) {
				processReference(client, bundleEntries, addedResource, addedPractitioner, composition, reference, addToSection);
			}

			// Add Attester
			for (CompositionAttesterComponent attester : composition.getAttester()) {
				processReference(client, bundleEntries, addedResource, addedPractitioner, composition, attester.getParty(), addToSection);
			}

			// Add custodian
			processReference(client, bundleEntries, addedResource, addedPractitioner, composition, composition.getCustodian(), addToSection);

			// Add related to composition
			for (CompositionRelatesToComponent relatedTo : composition.getRelatesTo()) {
				Type target = relatedTo.getTarget();
				if (target != null && target instanceof Reference) {
					processReference(client, bundleEntries, addedResource, addedPractitioner, composition, (Reference) target, addToSection);
				}
			}

			// Add event detail
			for (CompositionEventComponent event : composition.getEvent()) {
				for (Reference detail : event.getDetail()) {
					processReference(client, bundleEntries, addedResource, addedPractitioner, composition, detail, addToSection);
				}
			}

			for (SectionComponent section : composition.getSection()) {
				for (Reference reference : section.getEntry()) {
					// String referenceId = reference.getReferenceElement().getValue();
					addToSection = false;


					processReference(client, bundleEntries, addedResource, addedPractitioner, composition, reference, addToSection);
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

		Identifier bundleIdentifier = new Identifier();
		List<Extension> trackingNumExts = composition.getExtensionsByUrl("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number");
		for (Extension trackingNumExt : trackingNumExts) {
			Identifier trackingNumIdentifier = (Identifier) trackingNumExt.getValue();
			String code = trackingNumIdentifier.getType().getCodingFirstRep().getCode();
			String system = StringUtils.defaultString(trackingNumIdentifier.getSystem());
			String value = StringUtils.defaultString(trackingNumIdentifier.getValue());
			if ("mdi-case-number".equals(code)) {
				bundleIdentifier.setSystem(system);
				bundleIdentifier.setValue(value);
				break;
			} else if ("edrs-file-number".equals(code)) {
				bundleIdentifier.setSystem(system);
				bundleIdentifier.setValue(value);
			} 
		}

		if (!bundleIdentifier.isEmpty()) {
			retBundle.setIdentifier(bundleIdentifier);
		}

		retBundle.setTimestamp(new Date());

		if (saveIt) {
			client.create().resource(retBundle).encodedJson().prettyPrint().execute();
		}

		return retBundle;
	}

	// TODO: finish update after update API is developed.
	@Operation(name = "$update-mdi")
	public Parameters updateMdiDocumentOperation(RequestDetails theRequestDetails,
		@OperationParam(name = CompositionResourceProvider.SP_EDRS_FILE_NUMBER) TokenOrListParam theEdrsFileNumbers,
		@OperationParam(name = "mdi-document") Bundle theBundle) {

		if (theBundle != null) {
			OperationOutcome outcome = new OperationOutcome();
			String myFhirServerBase = theRequestDetails.getFhirServerBase();
			IGenericClient client = getFhirContext().newRestfulGenericClient(myFhirServerBase);
			OperationUtil.setupClientForAuth(client);

			// if tracking number is not in the parameter, get it from the bundle.
			if (theEdrsFileNumbers == null) {
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
					String identifierSystem = null;
					String identifierValue = null;
			
					String documentType = null;
					for (Coding coding : myType.getCoding()) {
						if (!coding.isEmpty()) {
							typeCoding = coding;
							String typeSystem = coding.getSystem();
							String typeCode = coding.getCode(); 
							if ("http://loinc.org".equalsIgnoreCase(typeSystem) && "86807-5".equalsIgnoreCase(typeCode)) {
								documentType = "MDI-DOCUMENT";
							} else if ("http://loinc.org".equalsIgnoreCase(typeSystem) && "11502-2".equalsIgnoreCase(typeCode)) {
								documentType = "LAB-DOCUMENT";
							}
						}
					}
			
					if (typeCoding == null || typeCoding.isEmpty()) {
						// We must have coding.
						outcome.addIssue().setSeverity(IssueSeverity.ERROR)
								.setDetails((new CodeableConcept()).setText("This composition type has no coding specified"));
						throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
					}

					if ("MDI-DOCUMENT".equals(documentType)) {
						// get case number.
						for (Extension ext : composition.getExtension()) {
							if ("http://hl7.org/fhir/us/mdi/StructureDefinition/Extension-tracking-number".equals(ext.getUrl())) {
								Identifier trackingIdentifier = (Identifier) ext.getValue();
								CodeableConcept tType = trackingIdentifier.getType();
								if (tType != null && !tType.isEmpty()) {
									for (Coding code : tType.getCoding()) {
										if ("http://hl7.org/fhir/us/mdi/CodeSystem/CodeSystem-mdi-codes".equals(code.getSystem())
											&& CompositionResourceProvider.EDRS_FILE_NUMBER.equals(code.getCode())) {
											identifierSystem = trackingIdentifier.getSystem();
											identifierValue = trackingIdentifier.getValue();
										}
									}
								}
							}
						}

						if (identifierValue == null) {
							outcome.addIssue().setSeverity(IssueSeverity.ERROR)
								.setDetails((new CodeableConcept()).setText("Tracking number for EDRS not found in the bundle"));
							throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
						}

						// Search the composition that has this tracking number.
						Bundle compositionBundle;
						if (identifierSystem != null && !identifierSystem.isEmpty()) {
							compositionBundle = client.search().forResource(Composition.class)
								.and(CompositionResourceProvider.EDRS_FILE_NUMBER
								.exactly()
								.systemAndCode(identifierSystem, identifierValue)
							).returnBundle(Bundle.class).execute();
						} else {
							compositionBundle = client.search().forResource(Composition.class)
								.and(CompositionResourceProvider.EDRS_FILE_NUMBER
								.exactly()
								.code(identifierValue)
							).returnBundle(Bundle.class).execute();
						}
						BundleEntryComponent entry = compositionBundle.getEntryFirstRep();
						if (entry.isEmpty()) {
							outcome.addIssue().setSeverity(IssueSeverity.ERROR)
									.setDetails((new CodeableConcept()).setText("EDRS file number (" + identifierSystem + "|" + identifierValue + ") is not found"));
							throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
						}
						
					} else {
						outcome.addIssue().setSeverity(IssueSeverity.ERROR)
								.setDetails((new CodeableConcept()).setText("This is not a MDI document"));
						throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
					}
				} else {
					outcome.addIssue().setSeverity(IssueSeverity.ERROR)
							.setDetails((new CodeableConcept()).setText("The first entry MUST be composition"));
					throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
				}
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

			return responseParameter;
		}

		return null;
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
