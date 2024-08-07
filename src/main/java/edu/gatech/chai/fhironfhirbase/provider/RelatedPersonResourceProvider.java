package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import jakarta.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.RelatedPerson;
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
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.operation.FhirbaseMapping;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class RelatedPersonResourceProvider extends BaseResourceProvider {
	protected FhirbaseMapping fhirbaseMapping;

	public RelatedPersonResourceProvider(FhirContext ctx) {
		super(ctx);

		setTableName(RelatedPersonResourceProvider.getType().toLowerCase());
		setMyResourceType(RelatedPersonResourceProvider.getType());
	}

	@PostConstruct
	private void postConstruct() {
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	public static String getType() {
		return "RelatedPerson";
	}

	@Override
	public Class<RelatedPerson> getResourceType() {
		return RelatedPerson.class;
	}

	@Create()
	public MethodOutcome createRelatedPerson(@ResourceParam RelatedPerson relatedPerson) {
		validateResource(relatedPerson);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource createdRelatedPerson = getFhirbaseMapping().create(relatedPerson, getResourceType());
			retVal.setId(createdRelatedPerson.getIdElement());
			retVal.setResource(createdRelatedPerson);
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
	public void deleteRelatedPerson(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	@Read()
	public IBaseResource readRelatedPerson(@IdParam IdType theId) {
		IBaseResource retVal = null;

		try {
			retVal = getFhirbaseMapping().read(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	@Update()
	public MethodOutcome updateRelatedPerson(@IdParam IdType theId, @ResourceParam RelatedPerson relatedPerson) {
		validateResource(relatedPerson);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource updatedRelatedPerson = getFhirbaseMapping().update(relatedPerson, getResourceType());
			retVal.setId(updatedRelatedPerson.getIdElement());
			retVal.setResource(updatedRelatedPerson);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	@Search()
	public IBundleProvider findRelatedPersonnByIds(
			@RequiredParam(name = RelatedPerson.SP_RES_ID) TokenOrListParam theRelatedPersonIds) {
		if (theRelatedPersonIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theRelatedPersonIds.getValuesAsQueryTokens()) {
			whereStatement += "rp.id = '" + theCondition.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM " + getTableName() + " rp " + whereStatement;
		String query = "SELECT * FROM " + getTableName() + " rp " + whereStatement;
		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findRelatedPersonByParams(
			@OptionalParam(name = Observation.SP_PATIENT, chainWhitelist = { "", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceOrListParam thePatients,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = getTableName() + " rp";

		if (thePatients != null) {
			fromStatement += " join patient p on rp.resource->'patient'->>'reference' = concat('Patient/', p.resource->>'id')";
		}

		if (thePatients != null) {
			for (ReferenceParam thePatient : thePatients.getValuesAsQueryTokens()) {
				String where = constructPatientWhereParameter(thePatient);
				if (thePatient.getChain() != null && !thePatient.getChain().isEmpty()) {
					fromStatement = constructFromStatementPatientChain(fromStatement, thePatient.getChain());
				}

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

		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	// TODO: Add more validation code here.
	private void validateResource(RelatedPerson relatedPerson) {
	}

	class MyBundleProvider extends FhirbaseBundleProvider {
		public MyBundleProvider(String query) {
			super(query);
			setPreferredPageSize(preferredPageSize);
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
