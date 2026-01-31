package edu.gatech.chai.fhironfhirbase.provider;

import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.FifoMemoryPagingProvider;

public class MyPagingProvider extends FifoMemoryPagingProvider {

    public MyPagingProvider(int theSize) {
        super(theSize);
    }
 
    @Override
	public synchronized String storeResultList(RequestDetails theRequestDetails, IBundleProvider theList) {
        String key = super.storeResultList(theRequestDetails, theList);

        FhirbaseBundleProvider myBundleProvider =(FhirbaseBundleProvider) theList;
        myBundleProvider.setUuid(key);

        return key;
	}
}
