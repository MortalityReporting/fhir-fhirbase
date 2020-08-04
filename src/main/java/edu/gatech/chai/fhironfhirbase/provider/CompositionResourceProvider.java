package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.codesystems.BundleType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
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
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.model.MyBundle;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;

@Service
@Scope("prototype")
public class CompositionResourceProvider extends BaseResourceProvider {
	private static final Logger logger = LoggerFactory.getLogger(CompositionResourceProvider.class);

	public CompositionResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(CompositionResourceProvider.getType().toLowerCase());
		setMyResourceType(CompositionResourceProvider.getType());
		getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
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

		return retVal;
	}

	@Delete()
	public void deleteComposition(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Search()
	public IBundleProvider findCompositionsById(
			@RequiredParam(name = Composition.SP_RES_ID) TokenOrListParam theCompositionIds, @Sort SortSpec theSort) {

		if (theCompositionIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theComposition : theCompositionIds.getValuesAsQueryTokens()) {
			whereStatement += "comp.id = " + theComposition.getValue() + " OR ";
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
			@OptionalParam(name = Composition.SP_PATIENT, chainWhitelist = { "", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceOrListParam thePatients,
			@OptionalParam(name = Composition.SP_SUBJECT, chainWhitelist = { "", USCorePatient.SP_NAME,
					USCorePatient.SP_IDENTIFIER }) ReferenceOrListParam theSubjects,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		String fromStatement = getTableName() + " comp";
		if (theOrTypes != null) {
			fromStatement = constructFromStatementPath(fromStatement, "types", "comp.resource->'type'->'coding'");
			String where = constructTypeWhereParameter(theOrTypes);
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theDate != null) {
			String where = constructDateWhereParameter(theDate, "comp", "date");
			if (where != null && !where.isEmpty()) {
				whereParameters.add(where);
			}
		}

		if (theSubjects != null) {
			for (ReferenceParam theSubject : theSubjects.getValuesAsQueryTokens()) {
				String where = constructSubjectWhereParameter(theSubject, "comp");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
		}

		if (thePatients != null) {
			for (ReferenceParam thePatient : thePatients.getValuesAsQueryTokens()) {
				String where = constructPatientWhereParameter(thePatient, "comp");
				if (where != null && !where.isEmpty()) {
					whereParameters.add(where);
				}
			}
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		String queryCount = "SELECT count(*) FROM " + fromStatement + whereStatement;
		String query = "SELECT * FROM " + fromStatement + whereStatement;

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
		if (!subjectResource.contentEquals("Patient")) {
			detailCode.setText("Subject (" + subjectResource + ") must be Patient");
			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}
	}

	@Operation(name = "$document", idempotent = true, bundleType = BundleTypeEnum.DOCUMENT)
	public Bundle generateDocumentOperation(RequestDetails theRequestDetails, @IdParam IdType theCompositionId,
			@OperationParam(name = "id") UriType theIdUri, @OperationParam(name = "persist") BooleanType thePersist,
			@OperationParam(name = "graph") UriType theGraph) {

		OperationOutcome outcome = new OperationOutcome();
		if (thePersist != null && !thePersist.isEmpty()) {
			if (thePersist.getValue()) {
				// We can't store this bundler. 
				outcome.addIssue().setSeverity(IssueSeverity.ERROR).setDetails(
						(new CodeableConcept()).setText("This server do not support Bundle persist"));
				throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
			}
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
				compositionId = theIdUri.getId();
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
		CodeableConcept myType = composition.getType();
		
		if (myType.isEmpty()) {
			// We can't generate a document without type. 
			outcome.addIssue().setSeverity(IssueSeverity.ERROR).setDetails(
					(new CodeableConcept()).setText("This composition has no type"));
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}
		
		Coding typeCoding = myType.getCodingFirstRep();
		if (typeCoding.isEmpty()) {
			// We must have coding.
			outcome.addIssue().setSeverity(IssueSeverity.ERROR).setDetails(
					(new CodeableConcept()).setText("This composition type has no coding"));
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

		if ("http://loinc.org".equalsIgnoreCase(typeSystem) && "64297-5".equalsIgnoreCase(typeCode)) {
			// This is a death certificate document. We need to add full resources in the section entries
			// to the resources.
			
			for (SectionComponent section: composition.getSection()) {
				for (Reference reference: section.getEntry()) {
					// get the reference. 
					Resource response = (Resource) client.read().resource(reference.getReferenceElement().getResourceType()).withId(reference.getReferenceElement().getIdPart()).encodedJson().execute();
					if (response == null || response.isEmpty()) {
						outcome.addIssue().setSeverity(IssueSeverity.ERROR).setDetails(
								(new CodeableConcept()).setText("resource (" + reference.getReferenceElement().getValue() + ") in Composition section not found"));
						throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
					}
					
					bundleEntry = new BundleEntryComponent();
					bundleEntry.setFullUrl(reference.getReferenceElement().getValue());
					bundleEntry.setResource(response);
					bundleEntries.add(bundleEntry);
				}
			}
		} else if ("http://loinc.org".equalsIgnoreCase(typeSystem) && "11502-2".equalsIgnoreCase(typeCode)) {
			// This is a lab report
		}

		Bundle retBundle = new Bundle();
		
		// This is generate Document operation. Thus, type must be Document.
		retBundle.setType(Bundle.BundleType.DOCUMENT);
		
		retBundle.setEntry(bundleEntries);
		
		return retBundle;
	}

	class MyBundleProvider extends FhirbaseBundleProvider implements IBundleProvider {
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

			try {
				retVal.addAll(getFhirbaseMapping().search(myQuery, getResourceType()));
			} catch (SQLException e) {
				e.printStackTrace();
			}

			return retVal;
		}
	}
}
