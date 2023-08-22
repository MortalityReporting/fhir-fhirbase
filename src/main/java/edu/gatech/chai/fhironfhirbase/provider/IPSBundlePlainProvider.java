package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Resource;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Sort;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class IPSBundlePlainProvider extends BasePlainProvider{
	private PatientResourceProvider patientResourceProvider;

	public IPSBundlePlainProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName("Bundle");
		setMyResourceType("Bundle");

		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	public static String getType() {	
		return "Bundle";
	}

	public Class<Bundle> getResourceType() {
		return Bundle.class;
	}

	/**
	 * The "@Create" annotation indicates that this method implements "create=type",
	 * which adds a new instance of a resource to the server.
	 */
	@Create(type = Bundle.class)
	public MethodOutcome createBundle(@ResourceParam Bundle theBundle) {
		validateResource(theBundle);
		if(patientResourceProvider == null){
			patientResourceProvider = new PatientResourceProvider(getFhirContext());
			patientResourceProvider.setFhirbaseMapping(this.getFhirbaseMapping());
			patientResourceProvider.setTableName(PatientResourceProvider.getType().toLowerCase());
			patientResourceProvider.setMyResourceType(PatientResourceProvider.getType());
		}
		//Persist patient if exists
		for(BundleEntryComponent bec:theBundle.getEntry()){
			Resource resource = bec.getResource();
			if(resource != null && resource instanceof USCorePatient){
				USCorePatient thePatient = (USCorePatient)resource;
				//Update Patient if exists
				if(thePatient.getId() != null && !thePatient.getId().isEmpty()){
					IBaseResource existingPatient = patientResourceProvider.readPatient(new IdType(thePatient.getId()));
					if(existingPatient != null){
						patientResourceProvider.updatePatient(new IdType(thePatient.getId()), thePatient);
					}
					else{
						persistRelatedPatient(theBundle, thePatient);
					}
				}
				//Else create patient
				else{
					persistRelatedPatient(theBundle, thePatient);
				}
			}
		}
		MethodOutcome retVal = new MethodOutcome();
		try {
			IBaseResource createdBundle = getFhirbaseMapping().create(theBundle, getResourceType());
			retVal.setId(createdBundle.getIdElement());
			retVal.setResource(createdBundle);
			retVal.setCreated(true);
		} catch (SQLException e) {
			retVal.setCreated(false);
			e.printStackTrace();
		}

		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

		return retVal;
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
	@Read(type = Bundle.class)
	public IBaseResource readBundle(@IdParam IdType theId) {
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
	public MethodOutcome updateBundle(@IdParam IdType theId, @ResourceParam Bundle theBundle) {
		validateResource(theBundle);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource updatedBundle = getFhirbaseMapping().update(theBundle, getResourceType());
			retVal.setId(updatedBundle.getIdElement());
			retVal.setResource(updatedBundle);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
	}

	@Delete(type = Bundle.class)
	public void deleteBundle(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);

	}

	/**
	 * The "@Search" annotation indicates that this method supports the search
	 * operation. You may have many different method annotated with this annotation,
	 * to support many different search criteria. This example searches by family
	 * name.
	 * 
	 * @param theFamilyName This operation takes one parameter which is the search
	 *                      criteria. It is annotated with the "@Required"
	 *                      annotation. This annotation takes one argument, a string
	 *                      containing the name of the search criteria. The datatype
	 *                      here is StringParam, but there are other possible
	 *                      parameter types depending on the specific search
	 *                      criteria.
	 * @return This method returns a list of Patients in bundle. This list may
	 *         contain multiple matching resources, or it may also be empty.
	 */
	@Search(type = Bundle.class, allowUnknownParams = true)
	public IBundleProvider findBundlesByParams(RequestDetails theRequestDetails,
			@OptionalParam(name = Bundle.SP_RES_ID) TokenParam theBundleId,
			@OptionalParam(name = Bundle.SP_IDENTIFIER) TokenParam theBundleIdentifier,
			@OptionalParam(name = Bundle.SP_COMPOSITION) ReferenceOrListParam theDocuments,
			@OptionalParam(name = Bundle.SP_MESSAGE) ReferenceOrListParam theMessages,
			@OptionalParam(name = Bundle.SP_TYPE) TokenOrListParam theTypes,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = "bundle b";

		if (theBundleId != null) {
			whereParameters.add("b.id = '" + theBundleId.getValue() + "'");
			returnAll = false;
		}

		if (theBundleIdentifier != null) {
			String system = theBundleIdentifier.getSystem();
			String value = theBundleIdentifier.getValue();

			if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
				whereParameters.add("b.resource->'identifier' @> '[{\"value\": \"" + value + "\",\"system\": \""
						+ system + "\"}]'::jsonb");
			} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
				whereParameters.add("b.resource->'identifier' @> '[{\"system\": \"" + system + "\"}]'::jsonb");
			} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
				whereParameters.add("b.resource->'identifier' @> '[{\"value\": \"" + value + "\"}]'::jsonb");
			}
			returnAll = false;
		}

		if (theDocuments != null) {
			String documentEntries = "";
			for (ReferenceParam documentReference : theDocuments.getValuesAsQueryTokens()) {
				if (!documentEntries.isEmpty()) {
					documentEntries += " OR ";
				}
				String documentId = documentReference.getResourceType() + "/" + documentReference.getIdPart();

				documentEntries += "b.resource->'entry'->0->>'fullUrl' like '%" + documentId + "%'";
			}

			if (!documentEntries.isEmpty()) {
				whereParameters.add("b.resource->>'type' = 'document'");
				whereParameters.add(documentEntries);

				returnAll = false;
			}
		}

		if (theMessages != null) {
			String messageEntries = "";
			for (ReferenceParam messageReference : theMessages.getValuesAsQueryTokens()) {
				if (!messageEntries.isEmpty()) {
					messageEntries += " OR ";
				}
				String messageId = messageReference.getResourceType() + "/" + messageReference.getIdPart();

				messageEntries += "b.resource->'entry'->0->>'fullUrl' like '%" + messageId + "%'";
			}

			if (!messageEntries.isEmpty()) {
				whereParameters.add("b.resource->>'type' = 'message'");
				whereParameters.add(messageEntries);

				returnAll = false;
			}
		}

		if (theTypes != null) {
			String typeOr = "";
			for (TokenParam type : theTypes.getValuesAsQueryTokens()) {
				String typeUrl = type.getSystem();
				if (typeUrl != null && !typeUrl.isBlank()) {
					// Search Raven extension type code.
					fromStatement = constructFromStatementPath(fromStatement, "types", "b.resource->'_type'->'extension'");
					typeOr = constructTypeToWhereParam(type, typeOr,  "url", "valueCode");
				} else {
					if (typeOr.isBlank()) {
						typeOr = "b.resource->>'type' = '" + type.getValue() + "'";
					} else {
						typeOr += " or b.resource->>'type' = '" + type.getValue() + "'";
					}	
				}
			}

			// extended code search
			if (!typeOr.isBlank()) {
				typeOr = "(" + typeOr + ")";
				whereParameters.add(typeOr);
				returnAll = false;
			}
		}

		// Complete Query.
		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll && (whereStatement == null || whereStatement.isEmpty())) {
			return null;
		}

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;
		MyBundleProvider myBundleProvider = new MyBundleProvider(query, null, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;

	}

	/**
	 * This method just provides simple business validation for resources we are
	 * storing.
	 * 
	 * @param thePatient The patient to validate
	 */
	private void validateResource(Bundle theBundle) {
		/*
		 * Our server will have a rule that patients must have a family name or we will
		 * reject them
		 * 
		 * commenting out this name validation as VRDR for MDI may not have names.
		 */
//		if (thePatient.getNameFirstRep().getFamily().isEmpty()) {
//			OperationOutcome outcome = new OperationOutcome();
//			CodeableConcept detailCode = new CodeableConcept();
//			detailCode.setText("No family name provided, Patient resources must have at least one family name.");
//			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
//			throw new UnprocessableEntityException(getFhirContext(), outcome);
//		}
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
			List<IBaseResource> retv = new ArrayList<IBaseResource>();

			String myQuery = query;			
			if (toIndex - fromIndex > 0) {
				myQuery += " LIMIT " + (toIndex - fromIndex) + " OFFSET " + fromIndex;
			}

			try {
				retv.addAll(getFhirbaseMapping().search(myQuery, getResourceType()));
			} catch (SQLException e) {
				e.printStackTrace();
			}

			return retv;
		}

	}

	public PatientResourceProvider getPatientResourceProvider() {
		return this.patientResourceProvider;
	}

	public void setPatientResourceProvider(PatientResourceProvider patientResourceProvider) {
		this.patientResourceProvider = patientResourceProvider;
	}

	private Bundle persistRelatedPatient(Bundle bundle, USCorePatient patient){
		MethodOutcome methodOutcome = patientResourceProvider.createPatient(patient);
		patient.setId(methodOutcome.getId());
		return bundle;
	}
}
