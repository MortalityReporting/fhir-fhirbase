package edu.gatech.chai.fhironfhirbase.provider;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Location;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Sort;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.operation.FhirbaseMapping;

@Service
public class LocationResourceProvider extends BaseResourceProvider {
	protected FhirbaseMapping fhirbaseMapping;
	private int preferredPageSize = 30;

	public LocationResourceProvider () {
		super();
	}
	
	@PostConstruct
    private void postConstruct() {
		getFhirbaseMapping().setFhirClass(Location.class);
		getFhirbaseMapping().setTableName(LocationResourceProvider.getType().toLowerCase());
		setMyResourceType(LocationResourceProvider.getType());
		
		getTotalSize("SELECT count(*) FROM "+getFhirbaseMapping().getTableName()+";");
	}

	public static String getType() {
		return "Location";
	}

	@Override
	public Class<Location> getResourceType() {
		return Location.class;
	}
	
	@Create()
	public MethodOutcome createLocation(@ResourceParam Location location) {
		return create(location);
	}
	
	@Delete()
	public void deleteLocation(@IdParam IdType theId) {
		delete(theId);
	}

	@Read()
	public IBaseResource readLocation(@IdParam IdType theId) {
		return read(theId, getResourceType(), "location");
	}

	@Update()
	public MethodOutcome updateLocation(@IdParam IdType theId, @ResourceParam Location location) {
		validateResource(location);

		return update(theId, location, getResourceType());
	}

	@Search()
	public IBundleProvider findLocationByIds(
			@RequiredParam(name = Location.SP_RES_ID) TokenOrListParam theLocationIds) {
		if (theLocationIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theCondition : theLocationIds.getValuesAsQueryTokens()) {
			whereStatement += "lo.id = " + theCondition.getValue() + " OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM location lo " + whereStatement;
		String query = "SELECT * FROM location lo " + whereStatement;
		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findLocationByParams(@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = "location lo";

		String whereStatement = constructWhereStatement(whereParameters, theSort);		
		
		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);

		return myBundleProvider;
	}
	
	// TODO: Add more validation code here.
	private void validateResource(Location location) {
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
