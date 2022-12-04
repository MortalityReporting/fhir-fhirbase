/*
 * Filename: /Users/mc142/Documents/workspace/mortality-reporting/raven-fhir-server-dev/fhir-fhirbase/src/main/java/edu/gatech/chai/fhironfhirbase/utilities/OperationUtil.java
 * Path: /Users/mc142/Documents/workspace/mortality-reporting/raven-fhir-server-dev/fhir-fhirbase/src/main/java/edu/gatech/chai/fhironfhirbase/utilities
 * Created Date: Tuesday, November 29th 2022, 12:29:38 am
 * Author: Myung Choi
 * 
 * Copyright (c) 2022 GTRI - Health Emerging and Advanced Technologies (HEAT)
 */
package edu.gatech.chai.fhironfhirbase.utilities;

import java.util.UUID;

import org.hl7.fhir.r4.model.Identifier;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;

public class OperationUtil {
	public static FhirContext MyFhirContext = FhirContext.forR4();
	
    public static void setupClientForAuth(IGenericClient client) {
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
	}

	public static String myHostUrl() {
		String myUrl = System.getenv("INTERNAL_FHIR_REQUEST_URL");
		if (myUrl == null || myUrl.isEmpty()) {
			myUrl = "http://localhost:8080/fhir";
		}

		return myUrl;
	}

	public static String RAVEN_TOX_SYSTEM = "urn:cms:toxicology:raven";
	public static String RAVEN_DEATH_REPORTING_SYSTEM = "urn:cms:death-reporting:raven";

	public static Identifier generateIdentifier(String system) {
		Identifier identifier = new Identifier();
		if (system != null && !system.isEmpty()) {
			identifier.setSystem(system);
		}
		identifier.setValue(UUID.randomUUID().toString());

		return identifier;
	}
}
