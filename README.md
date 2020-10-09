# fhir-fhirbase
FHIR Resource providers and database connector to fhirBase. This library handles all the VRDR required resources with full CRUD operations. 

## Installation
This is a jar file so that it cannot be executed without being instantiated by another software module. All the resource providers MUST be instantiated by the software module that will be using this library. Do the follows in the fhir-fhirbase folder to generate fhir-fhirbase jar file.

```
mvn clean install
```
The jar file should be created in the target/ directory with the filename, "fhir-fhirbase-version.jar". And, the mvn command will install the jar file in the local mvn library. This jar file can be included by adding the following lines to the dependency section in the pom.xml file (for maven project). For non-maven project, the jar file can be used. 

```
	<dependency>
		<groupId>edu.gatech.chai</groupId>
		<artifactId>fhir-fhirbase</artifactId>
		<version>put_version_info_here</version>
	</dependency>
```

## Additional operation API implemented.
fhir-fhirbase is implemented based on the HAPI FHIR library with annotations to be linked to the HAPI FHIR API routes. Most of CRUD operations are implemented and work as defined in the FHIR R4 specification. There are three additional operations implemented to support VRDR community.

1. Transaction Operation: The transaction API is https://<base URI>/  Payload must be batch type Bundle
2. VRDR Operation: This is similar to #1. The payload MUST be VRDR Bundle. And, the bundle type must be a document.
3. $document is implemented to support VRDR generation from Composition resource. 
