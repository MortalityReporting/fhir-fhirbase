package edu.gatech.chai.fhironfhirbase.provider;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.operation.FhirbaseMapping;

@Service
public class BundleResourceProvider extends BaseResourceProvider {

	protected FhirbaseMapping fhirbaseMapping;
	private int preferredPageSize = 30;

	public BundleResourceProvider () {
		super();
	}
	
	@PostConstruct
    private void postConstruct() {
		getFhirbaseMapping().setFhirClass(Bundle.class);
		getFhirbaseMapping().setTableName(BundleResourceProvider.getType().toLowerCase());
		setMyResourceType(BundleResourceProvider.getType());
		
		getTotalSize("SELECT count(*) FROM "+getFhirbaseMapping().getTableName()+";");
	}

	public static String getType() {
		return "Bundle";
	}

	@Override
	public Class<Bundle> getResourceType() {
		// TODO Auto-generated method stub
		return Bundle.class;
	}

	@Create()
	public MethodOutcome createBundle(@ResourceParam Bundle bundle) {
		return create(bundle);
	}
	
	@Delete()
	public void deleteBundle(@IdParam IdType theId) {
		delete(theId);
	}

	@Read()
	public IBaseResource readBundle(@IdParam IdType theId) {
		return read(theId, getResourceType(), "buncle");
	}

	@Update()
	public MethodOutcome updateBundle(@IdParam IdType theId, @ResourceParam Bundle bundle) {
		validateResource(bundle);

		return update(theId, bundle, getResourceType());
	}

	@Search()
	public IBundleProvider findBundleByIds(
			@RequiredParam(name = Bundle.SP_RES_ID) TokenOrListParam theBundleIds) {
		if (theBundleIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theBundleIds.getValuesAsQueryTokens()) {
			whereStatement += "b.id = " + theCondition.getValue() + " OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM bundle b " + whereStatement;
		String query = "SELECT * FROM bundle b " + whereStatement;
		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

//	@Search()
//	public IBundleProvider findBundleByParams(@OptionalParam(name = Bundle.SP_CODE) TokenOrListParam theOrCodes,
//			@OptionalParam(name = Bundle.SP_PATIENT, chainWhitelist = { "", USCorePatient.SP_NAME }) ReferenceOrListParam thePatients,
//			@Sort SortSpec theSort) {
//
//		List<String> whereParameters = new ArrayList<String>();
//		String fromStatement = "list l";
//		if (theOrCodes != null) {
//			String where = constructCodeWhereParameter(theOrCodes, fromStatement, "l.resource->'code'");
//			if (where != null && !where.isEmpty()) {
//				whereParameters.add(where);
//			}
//		}
//
//		if (theSubjects != null) {
//			for (ReferenceParam theSubject : theSubjects.getValuesAsQueryTokens()) {
//				String where = constructSubjectWhereParameter(theSubject, "l");
//				if (where != null && !where.isEmpty()) {
//					whereParameters.add(where);
//				}
//			}
//		}
//
//		if (thePatients != null) {
//			for (ReferenceParam thePatient : thePatients.getValuesAsQueryTokens()) {
//				String where = constructPatientWhereParameter(thePatient, "l");
//				if (where != null && !where.isEmpty()) {
//					whereParameters.add(where);
//				}
//			}
//		}
//
//		String whereStatement = constructWhereStatement(whereParameters, theSort);		
//		
//		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
//		String query = "SELECT * FROM " + fromStatement + whereStatement;
//
//		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
//		myBundleProvider.setTotalSize(getTotalSize(queryCount));
//		myBundleProvider.setPreferredPageSize(preferredPageSize);
//
//		return myBundleProvider;
//	}
	
	// TODO: Add more validation code here.
	private void validateResource(Bundle bundle) {
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
		public MyBundleProvider(String query) {
			super(query);
			setPreferredPageSize(preferredPageSize);
		}

		@Override
		public List<IBaseResource> getResources(int fromIndex, int toIndex) {
			// _Include
			// TODO: do this later
			List<String> includes = new ArrayList<String>();

			if (toIndex - fromIndex > 0) {
				query += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			return search(query, getResourceType());
		}

	}
}
