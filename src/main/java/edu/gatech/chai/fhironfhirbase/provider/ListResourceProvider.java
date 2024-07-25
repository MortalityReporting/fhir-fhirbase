package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.ListResource;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
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
import ca.uhn.fhir.rest.param.ReferenceAndListParam;
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.operation.FhirbaseMapping;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class ListResourceProvider extends BaseResourceProvider {
	protected FhirbaseMapping fhirbaseMapping;
	private int preferredPageSize = 30;

	public ListResourceProvider(FhirContext ctx) {
		super(ctx);

		setTableName(ListResourceProvider.getType().toLowerCase());
		setMyResourceType(ListResourceProvider.getType());
	}

	@PostConstruct
	private void postConstruct() {
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	public static String getType() {
		return "List";
	}

	@Override
	public Class<ListResource> getResourceType() {
		return ListResource.class;
	}

	@Create()
	public MethodOutcome createListResource(@ResourceParam ListResource listResource) {
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource createdList = getFhirbaseMapping().create(listResource, getResourceType());
			retVal.setId(createdList.getIdElement());
			retVal.setResource(createdList);
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
	public void deleteListResource(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	@Read()
	public IBaseResource readListResource(@IdParam IdType theId) {
		IBaseResource retVal = null;
		
		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Update()
	public MethodOutcome updateListResource(@IdParam IdType theId, @ResourceParam ListResource listResource) {
		validateResource(listResource);
		MethodOutcome retVal = new MethodOutcome();
		
		try {
			IBaseResource updatedList = getFhirbaseMapping().update(listResource, getResourceType());
			retVal.setId(updatedList.getIdElement());
			retVal.setResource(updatedList);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return retVal;
	}

	@Search()
	public IBundleProvider findListResourceByIds(
			@RequiredParam(name = ListResource.SP_RES_ID) TokenOrListParam theListResourceIds) {
		if (theListResourceIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theListResourceIds.getValuesAsQueryTokens()) {
			whereStatement += "l.id = '" + theCondition.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM list l " + whereStatement;
		String query = "SELECT * FROM list l " + whereStatement;
		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findListResourceByParams(
			@OptionalParam(name = ListResource.SP_CODE) TokenOrListParam theOrCodes,
			@OptionalParam(name = ListResource.SP_SUBJECT, chainBlacklist = {"",
					USCorePatient.SP_NAME }) ReferenceAndListParam theSubjects,
			@OptionalParam(name = ListResource.SP_SOURCE) ReferenceOrListParam theSources,
			@OptionalParam(name = ListResource.SP_PATIENT, chainWhitelist = { "",
					USCorePatient.SP_NAME }) ReferenceAndListParam thePatients,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = "list l";

		if (theSubjects != null || thePatients != null) {
			fromStatement += " join patient p on l.resource->'subject'->>'reference' = concat('Patient/', p.resource->>'id')";
	
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
		
		if (theOrCodes != null) {
			fromStatement = constructFromStatementPath(fromStatement, "codings", "l.resource->'code'->'coding'");
			String where = constructCodeWhereParameter(theOrCodes);
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
			returnAll = false;
		}

		if (theSources != null) {
			for (ReferenceParam theSource : theSources.getValuesAsQueryTokens()) {
				String where = constructReferenceWhereParameter(theSource, "l", "source");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
			returnAll = false;
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll) {
			if (whereStatement == null || whereStatement.isEmpty()) return null;
		}
		
		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	// TODO: Add more validation code here.
	private void validateResource(ListResource listResource) {
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
		public MyBundleProvider(String query) {
			super(query);
			setPreferredPageSize(preferredPageSize);
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			List<IBaseResource> retVal = new ArrayList<IBaseResource>();
			
			// _Include
			// TODO: do this later
			List<String> includes = new ArrayList<String>();

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
