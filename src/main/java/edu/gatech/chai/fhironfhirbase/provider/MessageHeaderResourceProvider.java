package edu.gatech.chai.fhironfhirbase.provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.MessageHeader;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
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
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class MessageHeaderResourceProvider extends BaseResourceProvider {
	private static final Logger logger = LoggerFactory.getLogger(MessageHeaderResourceProvider.class);

	public MessageHeaderResourceProvider(FhirContext ctx) {
		super(ctx);
	}

	@PostConstruct
	private void postConstruct() {
		setTableName(MessageHeaderResourceProvider.getType().toLowerCase());
		setMyResourceType(MessageHeaderResourceProvider.getType());

		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}


	public static String getType() {
		return "MessageHeader";
	}
	
	@Override
	public Class<MessageHeader> getResourceType() {
		return MessageHeader.class;
	}

	@Create()
	public MethodOutcome createMessageHeader(@ResourceParam MessageHeader theMessageHeader) {
		validateResource(theMessageHeader);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource createdObservation = getFhirbaseMapping().create(theMessageHeader, getResourceType());
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
	public void deleteMessageHeader(@IdParam IdType theId) {
		try {
			getFhirbaseMapping().delete(theId, getResourceType(), getTableName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		int totalSize = getTotalSize("SELECT count(*) FROM " + getTableName() + ";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	@Search()
	public IBundleProvider findMessageHeaderById(
			@RequiredParam(name = MessageHeader.SP_RES_ID) TokenOrListParam theMessageHeaderIds,
			@Sort SortSpec theSort) {

		if (theMessageHeaderIds == null) {
			return null;
		}

		String whereStatement = "WHERE ";
		for (TokenParam theMessageHeader : theMessageHeaderIds.getValuesAsQueryTokens()) {
			whereStatement += "mh.id = '" + theMessageHeader.getValue() + "' OR ";
		}

		whereStatement = whereStatement.substring(0, whereStatement.length() - 4);

		String queryCount = "SELECT count(*) FROM " + getTableName() + " mh " + whereStatement;
		String query = "SELECT * FROM " + getTableName() + " mh " + whereStatement;

		MyBundleProvider myBundleProvider = new MyBundleProvider(query, null, null);
		myBundleProvider.setTotalSize(getTotalSize(queryCount));
		myBundleProvider.setPreferredPageSize(preferredPageSize);
		return myBundleProvider;
	}

	@Search()
	public IBundleProvider findMessageHeaderByParams(
			@OptionalParam(name = MessageHeader.SP_SOURCE) StringParam theSource,
			@OptionalParam(name = MessageHeader.SP_SOURCE_URI) StringParam theSourceUri,
			@OptionalParam(name = MessageHeader.SP_FOCUS) ReferenceOrListParam theFocusReferences,
			@Sort SortSpec theSort) {

		List<String> whereParameters = new ArrayList<String>();
		boolean returnAll = true;
		
		String fromStatement = getTableName() + " mh";
		if (theSource != null) {
			whereParameters.add("mh.resource->'source'->>'name' = '" + theSource.getValue() + "'");
			returnAll = false;
		}

		if (theSourceUri != null) {
			whereParameters.add("mh.resource->'source'->>'endpoint' = '" + theSource.getValue() + "'");
			returnAll = false;
		}

		if (theFocusReferences != null) {
			fromStatement = constructFromStatementPath(fromStatement, "focuses", "mh.resource->'focus'");

			String whereFocuses = "";
			for (ReferenceParam focusReference : theFocusReferences.getValuesAsQueryTokens()) {
				if (!whereFocuses.isEmpty()) {
					whereFocuses += " OR ";
				}
				
				whereFocuses += "focuses @> '{\"reference\": \"" + focusReference.getValue() + "\"}'::jsonb";
			}

			whereParameters.add(whereFocuses);
		}

		String whereStatement = constructWhereStatement(whereParameters, theSort);

		if (!returnAll && (whereStatement == null || whereStatement.isEmpty())) {
			 return null;
		}

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
	public IBaseResource readMessageHeader(@IdParam IdType theId) {
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
	public MethodOutcome updateMessageHeader(@IdParam IdType theId, @ResourceParam MessageHeader theMessageHeader) {
		validateResource(theMessageHeader);
		MethodOutcome retVal = new MethodOutcome();

		try {
			IBaseResource updatedObservation = getFhirbaseMapping().update(theMessageHeader, getResourceType());
			retVal.setId(updatedObservation.getIdElement());
			retVal.setResource(updatedObservation);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return retVal;
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
	
	private void validateResource(MessageHeader theMessageHeader) {
		OperationOutcome outcome = new OperationOutcome();
		CodeableConcept detailCode = new CodeableConcept();
		if (theMessageHeader.getSource().isEmpty()) {
			detailCode.setText("Missing source in MessageHeader resource.");
			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		if (theMessageHeader.getEventCoding().isEmpty() && theMessageHeader.getEventUriType().isEmpty()) {
			detailCode.setText("Event[x] is empty in MessageHeader resource");
			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

		if (theMessageHeader.getFocus().size() == 0) {
			detailCode.setText("focus cannot be empty in MessageHeader resource for MDI FHIR IG");
			outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
			throw new UnprocessableEntityException(FhirContext.forR4(), outcome);
		}

	}

}
