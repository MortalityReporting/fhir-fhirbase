package edu.gatech.chai.fhironfhirbase.provider;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.beans.factory.annotation.Autowired;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.ParamPrefixEnum;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.operation.FhirbaseMapping;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;
import edu.gatech.chai.fhironfhirbase.utilities.ThrowFHIRExceptions;

public abstract class BaseResourceProvider implements IResourceProvider {
	protected int preferredPageSize = 30;
	
	private FhirbaseMapping fhirbaseMapping;
	private String myResourceType;
	private String tableName;
	private FhirContext ctx;
	
	public BaseResourceProvider(FhirContext ctx) {
		this.ctx = ctx;
	}

	@Autowired
	public final void setFhirbaseMapping(FhirbaseMapping fhirbaseMapping) {
		fhirbaseMapping.setCtx(getFhirContext());
		this.fhirbaseMapping = fhirbaseMapping;
	}
	
	public FhirContext getFhirContext() {
		return this.ctx;
	}
	
	public void setFhirContext(FhirContext ctx) {
		this.ctx = ctx;
	}
	
	public String getMyResourceType() {
		return this.myResourceType;
	}
	
	public void setMyResourceType(String myResourceType) {
		this.myResourceType = myResourceType;
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public FhirbaseMapping getFhirbaseMapping() {
		return this.fhirbaseMapping;
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

	protected String constructFromStatement(TokenOrListParam tokenList, String fromStatement, String alias, String pathToCoding) {
		List<TokenParam> tokens = tokenList.getValuesAsQueryTokens();
		if (tokens.size() > 0) {
			if (!fromStatement.contains(alias)) {
				fromStatement += ", jsonb_array_elements("+pathToCoding+"->'coding') " + alias;
			}
		}
		
		return fromStatement;
	}
	
	protected String constructTypeWhereParameter(TokenOrListParam theOrTypes) {
		List<TokenParam> types = theOrTypes.getValuesAsQueryTokens();

		String whereCodings = "";
		if (types.size() > 0) {
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

	protected String constructDateWhereParameter(DateParam theDate, String tableAlias, String column) {
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

	protected String constructCodeWhereParameter(TokenOrListParam theOrCodes) {
		List<TokenParam> codes = theOrCodes.getValuesAsQueryTokens();

		String whereCodings = "";
		if (codes.size() > 0) {
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
