# fhir-fhirbase
FHIRbase datastore implementation to store MDI FHIR data. This implements all the APIs and FHIR management. fhir-fhirbase requires fhirbase (postgresql) database, which needs to be deployed separagely. The following direction uses docker installation of fhirbase. 
```
sudo docker pull fhirbase/fhirbase:latest
sudo docker run -d -p 3000:3000 -p 5432:5432 --restart unless-stopped fhirbase/fhirbase:latest
```
This will deploy the fhirbase postgresql database. Do the follow to get list of docker containers.
```
sudo docker ps -a
```
The docker id should be the first column of fhirbase/fhirbase:latest container. Copy the docker id and do the following command.
```
docker exec -it [container ID from previous step] /bin/bash
```
This will get you inside the docker image. Run psql to run the psql tool and create a database.
```
postgres@xxxxxxxxx:/$ psql
psql (10.5 (Debian 10.5-1.pgdg90+1))
Type "help" for help.

postgres=#CREATE <database name>
postgres=#\q
```
Now you created a database in the fhirbase postgresql database. Now, at the prompt (of container), run the follow,
```
fhirbase -d <database name> --fhir=4.0.0 init
```
Your database is ready for the FHIR server. FORTE FHIR server can be downloaded from github repo and built as a FHIR server. Do the following to clone the java application from github. 
```
git clone --recurse https://github.com/MortalityReporting/forte-fhir-server.git
```
After cloning the project, you can go into the forte-fhir-server folder and add envrionment variables in the Dockerfile. See the following example. 
```
ENV JDBC_URL="jdbc:postgresql://<fhirbase url>/<database name>"
ENV JDBC_USERNAME="postgres"
ENV JDBC_PASSWORD="postgres"
ENV SMART_INTROSPECTURL="<url for forte-fhir-server>/forte-fhir-server/smart/introspect"
ENV SMART_AUTHSERVERURL="<url for forte-fhir-server>/forte-fhir-server/smart/authorize"
ENV SMART_TOKENSERVERURL="<url for forte-fhir-server>/forte-fhir-server/smart/token"
ENV AUTH_BEARER="<static bearer token>"
ENV AUTH_BASIC="<basic auth - ex) client:secret>"
ENV FHIR_READONLY="<True of False>"
ENV SERVERBASE_URL="<forte server's fhir URL - ex) https://myurl.com/forte-fhir-server/fhir>"
```
Now, you are ready to build and run the container.
```
sudo docker build -t forte-fhir-server .
sudo docker run -d --restart unless-stopped --publish 8080:8080 forte-fhir-server
```
If you did not change the docker file, then your URL will be 
* HAPI Tester UI: http(s)://<host url>:8080/forte-fhir-server/
* FHIR API URL: http(s)://<host url>:8080/forte-fhir-server/fhir
* SMART on FHIR App Registration: http(s)://<host url>:8080/forte-fhir-server/smart/

