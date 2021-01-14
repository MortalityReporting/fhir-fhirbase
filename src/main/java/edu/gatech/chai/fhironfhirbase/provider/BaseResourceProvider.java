package edu.gatech.chai.fhironfhirbase.provider;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
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
	
	protected String constructFromStatementJson(String fromStatement, String alias, String json) {
		if (!fromStatement.contains(alias)) {
			fromStatement += ", json_array_elements("+json+") " + alias;
		}
		
		return fromStatement;
	}

	protected String constructFromStatementPath(String fromStatement, String alias, String path) {
		if (!fromStatement.contains(alias)) {
			fromStatement += ", jsonb_array_elements("+path+") " + alias;
		}
		
		return fromStatement;
	}

//	protected String constructFromStatementTokens(TokenOrListParam tokenList, String fromStatement, String alias, String pathToCoding) {
//		List<TokenParam> tokens = tokenList.getValuesAsQueryTokens();
//		if (tokens.size() > 0) {
//			if (!fromStatement.contains(alias)) {
//				fromStatement += ", jsonb_array_elements("+pathToCoding+") " + alias;
//			}
//		}
//		
//		return fromStatement;
//	}
	
	protected String constructIdentifierWhereParameter(TokenParam theToken) {
		return null;
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

	protected String constructDatePeriodWhereParameter(Date startDate, Date endDate, String tableAlias, String column) {
		Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String where = "";
		if (startDate != null) {
			String startDateString = formatter.format(startDate);
			where += "(" + tableAlias + ".resource->>'" + column + "')::timestamp >= '" + startDateString + "'::timestamp";
		}
		
		if (endDate != null) {
			String endDateString = formatter.format(endDate);
			if (where != null && !where.isEmpty()) {
				where += " AND ";
			}
			where += "(" + tableAlias + ".resource->>'" + column + "')::timestamp <= '" + endDateString + "'::timestamp";
		}
		
		return where;
	}

	protected String constructReferenceWhereParameter(ReferenceParam theSource, String tableAlias, String column) {
		if (theSource != null) {
			if (theSource.getResourceType() != null) { 
				return tableAlias + ".resource->'" + column + "'->>'reference' like '%" + theSource.getValue() + "%'";
			} 
		}
		return null;
	}

	protected String constructSubjectWhereParameter(ReferenceParam theSubject, String tableAlias) {
		if (theSubject != null) {
//			if (theSubject.getResourceType() != null && 
//					theSubject.getResourceType().equals(PatientResourceProvider.getType())) {
//				return constructPatientWhereParameter(theSubject, tableAlias);
//			} else {
//				// If resource is null, we assume Patient.
//				if (theSubject.getResourceType() == null) {
//					return constructPatientWhereParameter(theSubject, tableAlias);
//				} 
//			}

			if (theSubject.getResourceType() != null) { 
				return tableAlias + ".resource->'subject'->>'reference' like '%" + theSubject.getValue() + "%'";
			} else {
				return constructPatientWhereParameter(theSubject, tableAlias);
			}
			
		}
		
		return null;
	}
	
	protected String constructPatientWhereParameter(ReferenceParam thePatient, String tableAlias) {
		if (thePatient != null) {
			String patientChain = thePatient.getChain();
			if (patientChain != null) {
				if (USCorePatient.SP_NAME.equals(patientChain)) {
					String thePatientName = thePatient.getValue();
					String where = patientNameSearch(thePatientName, tableAlias);
					if (where != null && !where.isEmpty()) {
						return where;
					}
				} else if (USCorePatient.SP_IDENTIFIER.equals(patientChain)) {
					TokenParam identifierToken = thePatient.toTokenParam(getFhirContext());
					String where = identifierSearch(identifierToken, tableAlias);
					if (where != null && !where.isEmpty()) {
						return where;
					}
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

	protected String patientNameSearch(String name, String alias) {
		String sql = "SELECT * FROM patient WHERE resource->>name like '" + name + "'";
		List<IBaseResource> patientResources;
		
		String retVal = null;
		try {
			patientResources = fhirbaseMapping.search(sql, USCorePatient.class);
			for (IBaseResource patientResource: patientResources) {
				String id = patientResource.getIdElement().getIdPart();
				if (retVal == null) {
					retVal = alias + ".resource->'subject'->>'reference' like '%Patient/" + id + "%'";
				} else {
					retVal = retVal + " or " + alias + ".resource->'subject'->>'reference' like '%Patient/" + id + "%'";
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			ThrowFHIRExceptions.internalErrorException(e.getMessage());
		}

		return retVal;
	}
	
	protected String identifierSearch(TokenParam identifierToken, String alias) {
		String retVal = null;
		
		String sql = "SELECT * FROM patient p " + constructFromStatementPath("", "identifiers", "p.resource->'identifier'") + " WHERE ";
		List<IBaseResource> resources;
		
		String system = identifierToken.getSystem();
		String value = identifierToken.getValue();
		
		try {
			String where = null;
			if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
				where = "identifiers @> '{\"system\": \"" + system + "\", \"value\": \"" + value + "\"}'::jsonb";
			} else if (system != null && !system.isEmpty()) {
				where = "identifiers @> '{\"system\": \"" + system + "\"}'::jsonb";
			} else if (value != null && !value.isEmpty()){
				where = "identifiers @> '{\"value\": \"" + value + "\"}'::jsonb";
			}

			if (where == null) return null;
			
			sql += where; 
			resources = fhirbaseMapping.search(sql, USCorePatient.class);

			for (IBaseResource resource: resources) {
				String id = resource.getIdElement().getIdPart();
				
				if (retVal == null) {
					retVal = alias + ".resource->'subject'->>'reference' like '%Patient/" + id + "%'";
				} else {
					retVal = retVal + " or " + alias + ".resource->'subject'->>'reference' like '%Patient/" + id + "%'";
				}				
			}
		} catch (Exception e) {
			e.printStackTrace();
			ThrowFHIRExceptions.internalErrorException(e.getMessage());
		}

		return retVal;
	}

}
