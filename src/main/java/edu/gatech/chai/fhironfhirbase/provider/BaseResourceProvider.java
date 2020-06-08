package edu.gatech.chai.fhironfhirbase.provider;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Resource;
import org.springframework.beans.factory.annotation.Autowired;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.ParamPrefixEnum;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.operation.FhirbaseMapping;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;

public abstract class BaseResourceProvider implements IResourceProvider {
	protected int preferredPageSize = 30;
	
	private FhirbaseMapping fhirbaseMapping;
	private String myResourceType;
	
	public BaseResourceProvider() {
	}

	@Autowired
	public final void setFhirbaseMapping(FhirbaseMapping fhirbaseMapping) {
		fhirbaseMapping.setCtx(FhirContext.forR4());
		this.fhirbaseMapping = fhirbaseMapping;
	}
	
	public FhirbaseMapping getFhirbaseMapping() {
		return this.fhirbaseMapping;
	}
	
	public String getMyResourceType() {
		return this.myResourceType;
	}
	
	public void setMyResourceType(String myResourceType) {
		this.myResourceType = myResourceType;
	}
	
	public String constructOrderParams(SortSpec theSort) {
		String direction;

		if (theSort == null)
			return "";

		if (theSort.getOrder() != null)
			direction = theSort.getOrder().toString();
		else
			direction = "ASC";

		String parameter = theSort.getParamName();
		while (theSort.getChain() != null && theSort.getChain() != theSort) {
			theSort = theSort.getChain();
			parameter += ", " + theSort.getParamName();
		}
		String orderParam = parameter + " " + direction;

		return orderParam.trim();
	}

	protected int getTotalSize(String query) {
		int totalSize = 0;

		if (query == null || query.isEmpty()) {
			return 0;
		}

		try {
			totalSize = fhirbaseMapping.getSize(query);
			ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
		} catch (Exception e) {

			totalSize = 0;
			e.printStackTrace();
		}

		return totalSize;
	}

	protected MethodOutcome create(IBaseResource resource) {
		String idString = null;
		try {
			idString = fhirbaseMapping.create(resource);
		} catch (Exception e) {
			e.printStackTrace();
			ThrowFHIRExceptions.internalErrorException(e.getMessage());
		}

		return new MethodOutcome(new IdType(idString));
	}

	protected IBaseResource read(IdType theId, Class<? extends Resource> fhirClass, String tableName) {
		IBaseResource retval = null;

		try {
			retval = fhirbaseMapping.read(theId, fhirClass, tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (retval == null) {
			throw new ResourceNotFoundException(theId);
		}

		return retval;
	}

	protected MethodOutcome update(IdType theId, IBaseResource thePatient, Class<? extends Resource> fhirClass) {
		IBaseResource retVal = null;

		try {
			retVal = fhirbaseMapping.update(thePatient, fhirClass);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (retVal == null) {
			throw new ResourceNotFoundException(theId);
		}
		MethodOutcome mo = new MethodOutcome(retVal.getIdElement(), true);
		mo.setResource(retVal);
		return mo;
	}

	protected void delete(IdType theId) {
		try {
			fhirbaseMapping.delete(theId);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ResourceNotFoundException(theId);
		}
	}

	protected List<IBaseResource> search(String sql, Class<? extends Resource> fhirClass) {
		List<IBaseResource> retv = new ArrayList<IBaseResource>();

		try {
			List<IBaseResource> searchedResource = fhirbaseMapping.search(sql, fhirClass);
			retv.addAll(searchedResource);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return retv;

	}

	protected String constructWhereStatement(List<String> whereParameters, SortSpec theSort) {
		String whereStatement = "";
		if (whereParameters.size() > 0) {
			whereStatement = " WHERE ";
			for (String whereParameter : whereParameters) {
				whereStatement += whereParameter + " AND ";
			}

			whereStatement = whereStatement.substring(0, whereStatement.length() - 5);
		}

		String orderParams = constructOrderParams(theSort);
		if (orderParams != null && !orderParams.isEmpty()) {
			whereStatement += " ORDER BY " + orderParams;
		}

		return whereStatement;
	}

	protected String constructTypeWhereParameter(TokenOrListParam theOrTypes, String fromStatement, String pathToCoding) {
		List<TokenParam> types = theOrTypes.getValuesAsQueryTokens();

		String whereCodings = "";
		if (types.size() > 0) {
			if (!fromStatement.contains("types")) {
				fromStatement += ", jsonb_array_elements("+pathToCoding+"->'coding') types";
			}

			for (TokenParam type : types) {
				String system = type.getSystem();
				String value = type.getValue();

				if (!whereCodings.isEmpty()) {
					whereCodings += " OR ";
				}
				if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
					whereCodings += "types @> '{\"system\": \"" + system + "\", \"code\": \"" + value + "\"}'::jsonb";
				} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
					whereCodings += "types @> '{\"system\": \"" + system + "\"}'::jsonb";
				} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
					whereCodings += "types @> '{\"code\": \"" + value + "\"}'::jsonb";
				} else {
					whereCodings += "types @> '{\"system\": \"\", \"code\": \"\"}'::jsonb";
				}
			}

			if (!whereCodings.isEmpty()) {
				whereCodings = "(" + whereCodings + ")";
			}
		}
		
		return whereCodings;

	}

	protected String constructDateWhereParameter(DateParam theDate, String fromStatement, String tableAlias, String column) {
		Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateString = formatter.format(theDate.getValue());
		String inequality = "=";
		if (ParamPrefixEnum.EQUAL == theDate.getPrefix()) {
			inequality = "=";
		} else if (ParamPrefixEnum.GREATERTHAN == theDate.getPrefix()) {
			inequality = ">";
		} else if (ParamPrefixEnum.GREATERTHAN_OR_EQUALS == theDate.getPrefix()) {
			inequality = ">=";
		} else if (ParamPrefixEnum.LESSTHAN == theDate.getPrefix()) {
			inequality = "<";
		} else if (ParamPrefixEnum.LESSTHAN_OR_EQUALS == theDate.getPrefix()) {
			inequality = "<=";
		}
		
		return "("+tableAlias+".resource->>'"+column+"')::timestamp " + inequality + " '" + dateString + "'::timestamp";		
	}
	
	protected String constructSubjectWhereParameter(ReferenceParam theSubject, String tableAlias) {
		if (theSubject != null) {
			if (theSubject.getResourceType() != null && 
					theSubject.getResourceType().equals(PatientResourceProvider.getType())) {
				return constructPatientWhereParameter(theSubject, tableAlias);
			} else {
				// If resource is null, we assume Patient.
				if (theSubject.getResourceType() == null) {
					return constructPatientWhereParameter(theSubject, tableAlias);
				} 
			}
			
			return tableAlias + ".resource->'subject'->>'reference' like '%" + theSubject.getResourceType() + "/" + theSubject.getValue() + "%'";
		}
		
		return null;
	}
	
	protected String constructPatientWhereParameter(ReferenceParam thePatient, String tableAlias) {
		if (thePatient != null) {
			String patientChain = thePatient.getChain();
			if (patientChain != null) {
				if (USCorePatient.SP_NAME.equals(patientChain)) {
					String thePatientName = thePatient.getValue();
					String where = patientNameSearch(thePatientName);
					if (where != null && !where.isEmpty()) {
						return where;
					}
				} else if ("".equals(patientChain)) {
					return tableAlias + ".resource->'subject'->>'reference' like '%Patient/" + thePatient.getValue() + "%'";
				}
			} 
			
			return tableAlias + ".resource->'subject'->>'reference' like '%Patient/" + thePatient.getValue() + "%'";
		}

		return null;
	}

	protected String constructCodeWhereParameter(TokenOrListParam theOrCodes, String fromStatement, String pathToCoding) {
		List<TokenParam> codes = theOrCodes.getValuesAsQueryTokens();

		String whereCodings = "";
		if (codes.size() > 0) {
			if (!fromStatement.contains("codings")) {
				fromStatement += ", jsonb_array_elements("+pathToCoding+"->'coding') codings";
			}

			for (TokenParam code : codes) {
				String system = code.getSystem();
				String value = code.getValue();

				if (!whereCodings.isEmpty()) {
					whereCodings += " OR ";
				}
				if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
					whereCodings += "codings @> '{\"system\": \"" + system + "\", \"code\": \"" + value + "\"}'::jsonb";
				} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
					whereCodings += "codings @> '{\"system\": \"" + system + "\"}'::jsonb";
				} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
					whereCodings += "codings @> '{\"code\": \"" + value + "\"}'::jsonb";
				} else {
					whereCodings += "codings @> '{\"system\": \"\", \"code\": \"\"}'::jsonb";
				}
			}

			if (!whereCodings.isEmpty()) {
				whereCodings = "(" + whereCodings + ")";
			}
		}

		return whereCodings;
	}

	protected String patientNameSearch(String name) {
		String sql = "SELECT * FROM patient WHERE resource->>name like '" + name + "'";
		List<IBaseResource> patientResources;
		try {
			patientResources = fhirbaseMapping.search(sql, USCorePatient.class);
			if (patientResources.size() > 0) {
				IBaseResource patientResource = patientResources.get(0);
				String id = patientResource.getIdElement().getIdPart();
				return "d.resource->'patient'->>'reference' like '%Patient/" + id + "%'";
			} else {
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
			ThrowFHIRExceptions.internalErrorException(e.getMessage());
		}

		return null;
	}

}
