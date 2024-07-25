package edu.gatech.chai.fhironfhirbase.provider;

import jakarta.annotation.PostConstruct;

import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Service
@Scope("prototype")
public class ParametersResourceProvider extends BaseResourceProvider {
	private static final Logger logger = LoggerFactory.getLogger(ParametersResourceProvider.class);

	public ParametersResourceProvider(FhirContext ctx) {
		super(ctx);

		setTableName(ParametersResourceProvider.getType().toLowerCase());
		setMyResourceType(ParametersResourceProvider.getType());
	}

	@PostConstruct
    private void postConstruct() {
		int totalSize = getTotalSize("SELECT count(*) FROM "+ParametersResourceProvider.getType().toLowerCase()+";");
		ExtensionUtil.addResourceCount(getMyResourceType(), (long) totalSize);
	}

	@Override
	public Class<Parameters> getResourceType() {
		return Parameters.class;
	}

	public static String getType() {
		return "Parameters";
	}

	@Read()
	public Parameters readParameters(@IdParam IdType theId) {
		return null;
	}

}
