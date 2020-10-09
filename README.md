# fhir-fhirbase
FHIR Resource providers and database connector to fhirBase. This library handles all the VRDR required resources with full CRUD operations. 

## Installation
This is a jar file so that it cannot be execulted without being instantiated by Server module. All the resource providers MUST be instantiated by a software that will be using this library. Do the follows to generate fhir-fhirbase jar file in the fhir-fhirbase folder.

```
mvn clean install
```
The jar file should be created in the target/ directory with the filename equals to "fhir-fhirbase-<version>.jar". 

## Additional operation API implemented.
1. Transaction Operation: The transaction API is https://<base URI>/  Payload must be batch type Bundle
2. VRDR Operation: This is similar to #1. The payload MUST be VRDR Bundle. 
