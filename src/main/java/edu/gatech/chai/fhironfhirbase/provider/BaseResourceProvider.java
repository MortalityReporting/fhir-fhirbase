package edu.gatech.chai.fhironfhirbase.provider;

import java.text.DateFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.IElement;
import ca.uhn.fhir.model.base.composite.BaseIdentifierDt;
import ca.uhn.fhir.model.primitive.StringDt;
import ca.uhn.fhir.model.primitive.UriDt;
import ca.uhn.fhir.rest.api.SortSpec;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.TokenClientParam;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.DateRangeParam;
import ca.uhn.fhir.rest.param.ParamPrefixEnum;
import ca.uhn.fhir.rest.param.ParameterUtil;
import ca.uhn.fhir.rest.param.ReferenceAndListParam;
import ca.uhn.fhir.rest.param.ReferenceOrListParam;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenOrListParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import edu.gatech.chai.fhironfhirbase.model.USCorePatient;
import edu.gatech.chai.fhironfhirbase.operation.FhirbaseMapping;

public abstract class BaseResourceProvider implements IResourceProvider {
	private static final Logger logger = LoggerFactory.getLogger(BaseResourceProvider.class);

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
		if (!whereParameters.isEmpty()) {
			whereStatement = " WHERE ";
			for (String whereParameter : whereParameters) {
				whereStatement += "(" + whereParameter + ") AND ";
			}

			whereStatement = whereStatement.substring(0, whereStatement.length() - 5);
		}

		String orderParams = constructOrderParams(theSort);
		if (orderParams != null && !orderParams.isEmpty()) {
			whereStatement += " ORDER BY " + orderParams;
		}

		return whereStatement;
	}
	
	protected String constructFromStatementPatientChain(String fromStatement, String chainName) {
		if (USCorePatient.SP_ADDRESS_CITY.equals(chainName)
			|| USCorePatient.SP_ADDRESS_COUNTRY.equals(chainName)
			|| USCorePatient.SP_ADDRESS_POSTALCODE.equals(chainName)
			|| USCorePatient.SP_ADDRESS_STATE.equals(chainName)
			|| USCorePatient.SP_ADDRESS_USE.equals(chainName)) {
			fromStatement = constructFromStatementPath(fromStatement, "addresses", "p.resource->'address'");
		} else if (USCorePatient.SP_FAMILY.equals(chainName)
			|| USCorePatient.SP_GIVEN.equals(chainName)) {
			fromStatement = constructFromStatementPath(fromStatement, "names", "p.resource->'name'");
		} else if (USCorePatient.SP_EMAIL.equals(chainName)
			|| USCorePatient.SP_PHONE.equals(chainName)
			|| USCorePatient.SP_TELECOM.equals(chainName)) {
			fromStatement = constructFromStatementPath(fromStatement, "telecoms", "p.resource->'contact'->'telecom'");
		} else if (USCorePatient.SP_IDENTIFIER.equals(chainName)) {
			fromStatement = constructFromStatementPath(fromStatement, "identifiers", "p.resource->'identifier'");
		}

		return fromStatement;
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

	protected String constructFromWherePatients(String fromStatement, List<String> whereParameters, ReferenceAndListParam theReferenceParts) {
		if (theReferenceParts != null) {
			for (ReferenceOrListParam theReferences : theReferenceParts.getValuesAsQueryTokens()) {
				String whereOr = "";
				for (ReferenceParam theReference : theReferences.getValuesAsQueryTokens()) {
					if (theReference.getResourceType() != null && !"Patient".equals(theReference.getResourceType())) {
						logger.warn("Unsupported resource found: " + theReference.getResourceType());
						return "";
					}

					String where = constructPatientWhereParameter(theReference);
					if (whereOr.isEmpty()) {
						whereOr = where;
					} else {
						whereOr += " or " + where;
					}
					if (theReference.getChain() != null && !theReference.getChain().isEmpty()) {
						fromStatement = constructFromStatementPatientChain(fromStatement, theReference.getChain());
					}	
				}

				if (whereOr != null && !whereOr.isEmpty()) {
					whereParameters.add(whereOr);
				}
			}
		}

		return fromStatement;
	}
	
	protected void addToWhereParemters(List<String> whereParameters, String where) {
		for (String whereParameter : whereParameters) {
			if (where.equals(whereParameter)) {
				return;
			}
		}

		whereParameters.add(where);
	}

	protected String constructTypesWhereParameter(TokenOrListParam theOrTypes) {
		return constructTypesWhereParameter(theOrTypes, "system", "code");
	}

	protected String constructTypeToWhereParam(TokenParam type, String whereCodings, String systemName, String valueName) {
		String system = type.getSystem();
		String value = type.getValue();

		if (whereCodings != null && !whereCodings.isEmpty()) {
			whereCodings += " OR ";
		} else {
			whereCodings = "";
		}
		
		if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
			whereCodings += "types @> '{\"" + systemName + "\": \"" + system + "\", \"" + valueName + "\": \"" + value + "\"}'::jsonb";
		} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
			whereCodings += "types @> '{\"" + systemName + "\": \"" + system + "\"}'::jsonb";
		} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
			whereCodings += "types @> '{\"" + valueName + "\": \"" + value + "\"}'::jsonb";
		} else {
			whereCodings += "types @> '{\"" + systemName + "\": \"\", \"" + valueName + "\": \"\"}'::jsonb";
		}

		return whereCodings;
	}

	protected String constructTypesWhereParameter(TokenOrListParam theOrTypes, String systemName, String valueName) {
		List<TokenParam> types = theOrTypes.getValuesAsQueryTokens();

		String whereCodings = "";
		if (!types.isEmpty()) {
			for (TokenParam type : types) {
				whereCodings = constructTypeToWhereParam(type, whereCodings, systemName, valueName);
				// String system = type.getSystem();
				// String value = type.getValue();

				// if (!whereCodings.isEmpty()) {
				// 	whereCodings += " OR ";
				// }
				// if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
				// 	whereCodings += "types @> '{\"" + systemName + "\": \"" + system + "\", \"" + valueName + "\": \"" + value + "\"}'::jsonb";
				// } else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
				// 	whereCodings += "types @> '{\"" + systemName + "\": \"" + system + "\"}'::jsonb";
				// } else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
				// 	whereCodings += "types @> '{\"" + valueName + "\": \"" + value + "\"}'::jsonb";
				// } else {
				// 	whereCodings += "types @> '{\"" + systemName + "\": \"\", \"" + valueName + "\": \"\"}'::jsonb";
				// }
			}

			if (!whereCodings.isEmpty()) {
				whereCodings = "(" + whereCodings + ")";
			}
		}
		
		return whereCodings;

	}

	protected String constructDateRangeWhereParameter(DateRangeParam theDateRange, String tableAlias, String column) {
		String dateWhere = "";

		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
		if (theDateRange.getLowerBound() != null && !theDateRange.getLowerBound().isEmpty()
			&& theDateRange.getUpperBound() != null && !theDateRange.getUpperBound().isEmpty()) {
			// We have both lower and upper.
			String lowerBound = formatter.format(theDateRange.getLowerBoundAsInstant());
			String upperBound = formatter.format(theDateRange.getUpperBoundAsInstant());
			dateWhere = tableAlias + ".resource->>'" + column + "' >= '" + lowerBound 
				+ "' and " + tableAlias + ".resource->>'" + column + "' <= '" + upperBound + "'";
		} else if (theDateRange.getLowerBound() != null && !theDateRange.getLowerBound().isEmpty()) {
			String lowerBound = formatter.format(theDateRange.getLowerBoundAsInstant());
			if (ParamPrefixEnum.GREATERTHAN_OR_EQUALS == theDateRange.getLowerBound().getPrefix()) {
				dateWhere = tableAlias + ".resource->>'" + column + "' >= '" + lowerBound + "'";
			} else if (ParamPrefixEnum.GREATERTHAN == theDateRange.getLowerBound().getPrefix()) {
				dateWhere = tableAlias + ".resource->>'" + column + "' > '" + lowerBound + "'";
			} else {
				dateWhere = tableAlias + ".resource->>'" + column + "' = '" + lowerBound + "'";
			}
		} else {
			String upperBound = formatter.format(theDateRange.getUpperBoundAsInstant());
			if (ParamPrefixEnum.LESSTHAN_OR_EQUALS == theDateRange.getUpperBound().getPrefix()) {
				dateWhere = tableAlias + ".resource->>'" + column + "' <= '" + upperBound + "'";
			} else if (ParamPrefixEnum.LESSTHAN == theDateRange.getUpperBound().getPrefix()) {
				dateWhere = tableAlias + ".resource->>'" + column + "' < '" + upperBound + "'";
			} else {
				dateWhere = tableAlias + ".resource->>'" + column + "' = '" + upperBound + "'";
			}
		}

		return dateWhere;
	}

	protected String constructDateRangeAliasPathWhere(DateRangeParam theDateRange, String aliasPath, String column) {
		String dateWhere = "";

		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
		if (theDateRange.getLowerBound() != null && !theDateRange.getLowerBound().isEmpty()
			&& theDateRange.getUpperBound() != null && !theDateRange.getUpperBound().isEmpty()) {
			// We have both lower and upper.
			String lowerBound = formatter.format(theDateRange.getLowerBoundAsInstant());
			String upperBound = formatter.format(theDateRange.getUpperBoundAsInstant());
			dateWhere = aliasPath + "->>'" + column + "' >= '" + lowerBound + "' and " + 
						aliasPath + "->>'" + column + "' <= '" + upperBound + "'";
		} else if (theDateRange.getLowerBound() != null && !theDateRange.getLowerBound().isEmpty()) {
			String lowerBound = formatter.format(theDateRange.getLowerBoundAsInstant());
			if (ParamPrefixEnum.GREATERTHAN_OR_EQUALS == theDateRange.getLowerBound().getPrefix()) {
				dateWhere = aliasPath + "->>'" + column + "' >= '" + lowerBound + "'";
			} else if (ParamPrefixEnum.GREATERTHAN == theDateRange.getLowerBound().getPrefix()) {
				dateWhere = aliasPath + "->>'" + column + "' > '" + lowerBound + "'";
			} else {
				dateWhere = aliasPath + "->>'" + column + "' = '" + lowerBound + "'";
			}
		} else {
			String upperBound = formatter.format(theDateRange.getUpperBoundAsInstant());
			if (ParamPrefixEnum.LESSTHAN_OR_EQUALS == theDateRange.getUpperBound().getPrefix()) {
				dateWhere = aliasPath + "->>'" + column + "' <= '" + upperBound + "'";
			} else if (ParamPrefixEnum.LESSTHAN == theDateRange.getUpperBound().getPrefix()) {
				dateWhere = aliasPath + "->>'" + column + "' < '" + upperBound + "'";
			} else {
				dateWhere = aliasPath + "->>'" + column + "' = '" + upperBound + "'";
			}
		}
		return dateWhere;
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
		
		return tableAlias+".resource->>'"+column+"' " + inequality + " '" + dateString + "'";		
	}

	protected String constructDatePeriodWhereParameter(Date startDate, Date endDate, String tableAlias, String column) {
		Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String where = "";
		if (startDate != null) {
			String startDateString = formatter.format(startDate);
			where += tableAlias + ".resource->>'" + column + "' >= '" + startDateString + "'";
		}
		
		if (endDate != null) {
			String endDateString = formatter.format(endDate);
			if (where != null && !where.isEmpty()) {
				where += " AND ";
			}
			where += tableAlias + ".resource->>'" + column + "' <= '" + endDateString + "'";
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

	protected String constructCanonicalWhereParameter(ReferenceParam theSource, String tableAlias, String column) {
		if (theSource != null) {
			if (theSource.getResourceType() != null) { 
				return tableAlias + ".resource->>'" + column + "' like '%" + theSource.getValue() + "%'";
			} 
		}
		return null;
	}
	
	protected String constructPatientWhereParameter(ReferenceParam thePatient) {
		String where = "";

		if (thePatient != null) {
			String theValue = thePatient.getValue();

			String patientChain = thePatient.getChain();
			if (patientChain != null) {
				if (USCorePatient.SP_NAME.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "lower(p.resource->>'name') like " + "lower('%" + theValue + "%')";
 					} else {
						where += " and lower(p.resource->>'name') like " + "lower('%" + theValue + "%')";
					}
				} else if (USCorePatient.SP_ADDRESS_CITY.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "lower(addresses::text)::jsonb @> lower('{\"city\":\""+ theValue +"\"}')::jsonb";
 					} else {
						where += " and lower(addresses::text)::jsonb @> lower('{\"city\":\""+ theValue +"\"}')::jsonb";
					}
				} else if (USCorePatient.SP_ADDRESS_COUNTRY.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "lower(addresses::text)::jsonb @> lower('{\"country\":\""+ theValue +"\"}')::jsonb";
 					} else {
						where += " and lower(addresses::text)::jsonb @> lower('{\"country\":\""+ theValue +"\"}')::jsonb";
					}
				} else if (USCorePatient.SP_ADDRESS_POSTALCODE.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "addresses @> '{\"postalCode\":\""+ theValue +"\"}'";
 					} else {
						where += " and addresses @> '{\"postalCode\":\""+ theValue +"\"}'";
					}
				} else if (USCorePatient.SP_ADDRESS_STATE.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "lower(addresses::text)::jsonb @> lower('{\"state\":\""+ theValue +"\"}')::jsonb";
 					} else {
						where += " and lower(addresses::text)::jsonb @> lower('{\"state\":\""+ theValue +"\"}')::jsonb";
					}
				} else if (USCorePatient.SP_ADDRESS_USE.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "addresses @> '{\"use\":\""+ theValue +"\"}'";
 					} else {
						where += " and addresses @> '{\"use\":\""+ theValue +"\"}'";
					}
				} else if (USCorePatient.SP_BIRTHDATE.equals(patientChain)) {
					DateParam theDateParam = thePatient.toDateParam(getFhirContext());
					String birthDateWhere = constructDateWhereParameter(theDateParam, "patient", "birthDate");
					if (where.isEmpty()) {
						where = birthDateWhere;
 					} else {
						where += " and " + birthDateWhere;
					}
				} else if (USCorePatient.SP_DEATH_DATE.equals(patientChain)) {
					DateParam theDateParam = thePatient.toDateParam(getFhirContext());
					String deathDateWhere = constructDateWhereParameter(theDateParam, "patient", "deceasedDateTime");
					if (where.isEmpty()) {
						where = deathDateWhere;
 					} else {
						where += " and " + deathDateWhere;
					}
				} else if (USCorePatient.SP_EMAIL.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "lower(telecoms::text)::jsonb @> lower('{\"system\":\"email\", \"value\":\""+ theValue +"\"}')::jsonb";
 					} else {
						where += " and lower(telecoms::text)::jsonb @> lower('{\"system\":\"email\", \"value\":\""+ theValue +"\"}')::jsonb";
					}
				} else if (USCorePatient.SP_FAMILY.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "lower(names::text)::jsonb @> lower('{\"family\":\""+ theValue +"\"}')::jsonb";
 					} else {
						where += " and lower(names::text)::jsonb @> lower('{\"family\":\""+ theValue +"\"}')::jsonb";
					}
				} else if (USCorePatient.SP_GENDER.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "p.resource->>'gender' = " + "'" + theValue + "'";
 					} else {
						where += " and p.resource->>'gender' = " + "'" + theValue + "'";
					}
				} else if (USCorePatient.SP_GIVEN.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "lower(names::text)::jsonb @> lower('{\"given\":[\""+ theValue + "\"]}')::jsonb";
 					} else {
						where += " and lower(names::text)::jsonb @> lower('{\"given\":[\""+ theValue + "\"]}')::jsonb";
					}
				} else if (USCorePatient.SP_PHONE.equals(patientChain)) {
					if (where.isEmpty()) {
						where = "telecoms @> '{\"system\":\"phone\", \"value\":\""+ theValue +"\"}'";
 					} else {
						where += " and telecoms @> '{\"system\":\"phone\", \"value\":\""+ theValue +"\"}'";
					}
				} else if (USCorePatient.SP_TELECOM.equals(patientChain)) {
					TokenParam tokenPatient = thePatient.toTokenParam(getFhirContext());
					if (where.isEmpty()) {
						where = "lower(telecoms::text)::jsonb @> lower('{\"system\":\"" + tokenPatient.getSystem() + "\", \"value\":\""+ tokenPatient.getValue() +"\"}')::jsonb";
 					} else {
						where += " and lower(telecoms::text)::jsonb @> lower('{\"system\":\"" + tokenPatient.getSystem() + "\", \"value\":\""+ tokenPatient.getValue() +"\"}')::jsonb";
					}
				} else if (USCorePatient.SP_IDENTIFIER.equals(patientChain)) {
					TokenParam identifierToken = thePatient.toTokenParam(getFhirContext());
					if (where.isEmpty()) {
						where = "identifiers @> '{\"system\":\"" + identifierToken.getSystem() + "\", \"value\":\""+ identifierToken.getValue() +"\"}'";
 					} else {
						where += " and identifiers @> '{\"system\":\"" + identifierToken.getSystem() + "\", \"value\":\""+ identifierToken.getValue() +"\"}'";
					}
				}
			} else {
				where = "p.resource->>'id' like '" + thePatient.getIdPart() + "'";
			}
		}

		return where;
	}

	protected String constructCodeWhereParameter(TokenOrListParam theOrCodes) {
		List<TokenParam> codes = theOrCodes.getValuesAsQueryTokens();

		String whereCodings = "";
		if (!codes.isEmpty()) {
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

	protected String constructIdentifierWhereParameter(TokenOrListParam theOrIdentifiers) {
		List<TokenParam> identifiers = theOrIdentifiers.getValuesAsQueryTokens();

		String whereCodings = "";
		if (!identifiers.isEmpty()) {
			for (TokenParam identifier : identifiers) {
				String system = identifier.getSystem();
				String value = identifier.getValue();

				if (!whereCodings.isEmpty()) {
					whereCodings += " OR ";
				}
				if (system != null && !system.isEmpty() && value != null && !value.isEmpty()) {
					whereCodings += "identifiers @> '{\"system\": \"" + system + "\", \"value\": \"" + value + "\"}'::jsonb";
				} else if (system != null && !system.isEmpty() && (value == null || value.isEmpty())) {
					whereCodings += "identifiers @> '{\"system\": \"" + system + "\"}'::jsonb";
				} else if ((system == null || system.isEmpty()) && value != null && !value.isEmpty()) {
					whereCodings += "identifiers @> '{\"value\": \"" + value + "\"}'::jsonb";
				} else {
					whereCodings += "identifiers @> '{\"system\": \"\", \"value\": \"\"}'::jsonb";
				}
			}

			if (!whereCodings.isEmpty()) {
				whereCodings = "(" + whereCodings + ")";
			}
		}

		return whereCodings;
	}

	protected boolean addTokenToIdentifierQuery (IQuery<IBaseBundle> query, TokenClientParam clientParam, StringOrListParam theTokenList) {
		boolean retVal = false;

		if (theTokenList == null) {
			return retVal;
		}

		List<BaseIdentifierDt> identifiers = new ArrayList<BaseIdentifierDt>();
		for (StringParam tokenParam : theTokenList.getValuesAsQueryTokens()) {
			String token = tokenParam.getValue();
			int barIndex = ParameterUtil.nonEscapedIndexOf(token, '|');
			String system = null;
			String value = null;
			if (barIndex != -1) {
				system = token.substring(0, barIndex);
				value = ParameterUtil.unescape(token.substring(barIndex + 1));
			} else {
				value = ParameterUtil.unescape(token);
			}

			BaseIdentifierDt identifierDt = new BaseIdentifierDt() {
				UriDt theSystem;
				StringDt theValue;

				@Override
				public boolean isEmpty() {
					return false;
				}

				@Override
				public <T extends IElement> List<T> getAllPopulatedChildElementsOfType(Class<T> theType) {
					return null;
				}

				@Override
				public UriDt getSystemElement() {
					return theSystem;
				}

				@Override
				public StringDt getValueElement() {
					return theValue;
				}

				@Override
				public BaseIdentifierDt setSystem(String theUri) {
					theSystem = new UriDt(theUri);
					return this;
				}

				@Override
				public BaseIdentifierDt setValue(String theString) {
					theValue = new StringDt(theString);
					return this;
				}
			};

			identifierDt.setSystem(system);
			identifierDt.setValue(value);
	
			identifiers.add(identifierDt);
		}

		if (identifiers.size() > 0) {
			query = query.and(clientParam.exactly().identifiers(identifiers));
			retVal = true;
		}

		return retVal;
	}

	protected void throwSimulatedOO(String errorCode) {
		OperationOutcome outcome = new OperationOutcome();
		CodeableConcept detailCode = new CodeableConcept();
		detailCode.setText(errorCode);
		outcome.addIssue().setSeverity(IssueSeverity.FATAL).setDetails(detailCode);
		throw new InvalidRequestException(errorCode, outcome);
	}
}
