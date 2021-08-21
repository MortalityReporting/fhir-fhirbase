package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.PractitionerRole;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.IncludeParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Sort;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class PractitionerRoleResourceProvider extends BaseResourceProvider {

	public PractitionerRoleResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(PractitionerRoleResourceProvider.getType().toLowerCase());
		setMyResourceType(PractitionerRoleResourceProvider.getType());

		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	public static String getType() {
		return "PractitionerRole";
	}

	@Override
	public Class<PractitionerRole> getResourceType() {
		return PractitionerRole.class;
	}
	
	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create()
	public MethodOutcome createPractitionerRole(@ResourceParam PractitionerRole thePractitionerRole) {
		validateResource(thePractitionerRole);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdPractitionerRole = getFhirbaseMapping().create(thePractitionerRole, getResourceType());
			retVal.setId(createdPractitionerRole.getIdElement());
			retVal.setResource(createdPractitionerRole);
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
	public void deletePractitionerRole(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	@Search()
	public IBundleProvider findPractitionerRolesByParams(
			@OptionalParam(name = PractitionerRole.SP_RES_ID) TokenOrListParam thePractitionerRoleIds,
			@OptionalParam(name = PractitionerRole.SP_IDENTIFIER) TokenParam thePractitionerRoleIdentifier,
			@OptionalParam(name = PractitionerRole.SP_ACTIVE) TokenParam theActive,
			@OptionalParam(name = PractitionerRole.SP_ORGANIZATION) ReferenceOrListParam theOrganizations,
			@OptionalParam(name = PractitionerRole.SP_PRACTITIONER) ReferenceOrListParam thePractitioners,
			@Sort SortSpec theSort,
			@IncludeParam(allow = {}) final Set<Include> theIncludes,
			@IncludeParam(reverse = true) final Set<Include> theReverseIncludes) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = "practitionerrole practrole";

		if (thePractitionerRoleIds != null) {
			for (TokenParam thePractitionerRoleId : thePractitionerRoleIds.getValuesAsQueryTokens()) {
				whereParameters.add("practrole.id = '" + thePractitionerRoleId.getValue() + "'");
			}
			returnAll = false;
		}
		
		if (thePractitionerRoleIdentifier != null) {
			String system = thePractitionerRoleIdentifier.getSystem();
			String value = thePractitionerRoleIdentifier.getValue();

			if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
				whereParameters.add("practrole.resource->'identifier' @> '[{\"value\": \"" + value + "\",\"system\": \""
						+ system + "\"}]'::jsonb");
			} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
				whereParameters.add("pract.resource->'identifier' @> '[{\"system\": \"" + system + "\"}]'::jsonb");
			} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
				whereParameters.add("pract.resource->'identifier' @> '[{\"value\": \"" + value + "\"}]'::jsonb");
			}
			returnAll = false;
		}

		if (theActive != null) {
			whereParameters.add("practrole.resource->>'active'=" + theActive.getValue());
			returnAll = false;
		}

		if (theOrganizations != null) {
			for (ReferenceParam theOrganization : theOrganizations.getValuesAsQueryTokens()) {
				String where = constructReferenceWhereParameter(theOrganization, "practrole", "organization");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
			returnAll = false;
		}

		if (thePractitioners != null) {
			for (ReferenceParam thePractitioner : thePractitioners.getValuesAsQueryTokens()) {
				String where = constructReferenceWhereParameter(thePractitioner, "practrole", "practitioner");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
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
	
	@Read()
	public IBaseResource readPractitioner(@IdParam IdType theId) {
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
	 * @param theId           This is the ID of the patient to update
	 * @param thePractitioner This is the actual resource to save
	 * @return This method returns a "MethodOutcome"
	 */
	@Update()
	public MethodOutcome updatePractitioner(@IdParam IdType theId, @ResourceParam PractitionerRole thePractitionerRole) {
		validateResource(thePractitionerRole);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedPractitionerRole = getFhirbaseMapping().update(thePractitionerRole, getResourceType());
			retVal.setId(updatedPractitionerRole.getIdElement());
			retVal.setResource(updatedPractitionerRole);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}
	
	/**
	 * This method just provides simple business validation for resources we are
	 * storing.
	 * 
	 * @param thePractitioner The thePractitioner to validate
	 */
	private void validateResource(PractitionerRole thePractitionerRole) {
		/*
		 * Our server will have a rule that practitioners must have a name or we will
		 * reject them
		 */
		// TODO: Add validation here
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
}
