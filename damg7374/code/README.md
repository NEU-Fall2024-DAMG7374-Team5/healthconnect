$SPARK_HOME/sbin/start-master.sh 

$SPARK_HOME/sbin/start-worker.sh spark://Adityas-MacBook-Pro.local:7077





$SPARK_HOME/sbin/stop-master.sh 
$SPARK_HOME/sbin/stop-worker.sh


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./AuraDBproperties/patientETLProperties.properties \
./data-pipeline/patientETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./AuraDBproperties/organizationETLProperties.properties \
./data-pipeline/organizationETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./AuraDBproperties/providerETLProperties.properties \
./data-pipeline/providerETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./AuraDBproperties/payerETLProperties.properties \
./data-pipeline/payerETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./AuraDBproperties/encounterETLProperties.properties \
./data-pipeline/encounterETL.py


//KG Final 
MATCH (patient:Patient{Id: '040394FF-A94B-6F67-F319-51C88916A8E6'})-[:HAD_ENCOUNTER]->(e:Encounter)
WITH patient, collect(e) AS encounters
UNWIND encounters AS encounter
OPTIONAL MATCH encounter_class = (encounter)-[:HAS_ENCOUNTER_CLASS]->(:EncounterClass)
OPTIONAL MATCH encounter_code = (encounter)-[:HAS_ENCOUNTER_CODE]->(:SNOMEDCT)
OPTIONAL MATCH diagnosis_code = (encounter)-[:HAS_DIAGNOSIS_CODE]->(:SNOMEDCT)

OPTIONAL MATCH (patient)-[:LIVES_IN_CITY]->(patientCity:City)
OPTIONAL MATCH (patientCity)-[:IS_PART_OF]->(patientCounty:County)
OPTIONAL MATCH (patientCounty)-[:IS_PART_OF]->(patientState:State)
OPTIONAL MATCH (patient)-[:HAS_GENDER]->(patientGender:Gender)
OPTIONAL MATCH (patient)-[:HAS_RACE]->(patientRace:Race)
OPTIONAL MATCH (patient)-[:HAS_ETHNICITY]->(patientEthnicity:Ethnicity)
OPTIONAL MATCH (patient)-[:HAS_ADDRESS]->(patientAddress:Address)
OPTIONAL MATCH (patientAddress)-[:IS_LOCATED_IN_CITY]->(patientAddressCity:City)

OPTIONAL MATCH (encounter)<-[:WAS_PROVIDER_FOR_ENCOUNTER]-(provider:Provider)
OPTIONAL MATCH (provider)-[:HAS_GENDER]->(providerGender:Gender)
OPTIONAL MATCH (provider)-[:HAS_ADDRESS]->(providerAddress:Address)
OPTIONAL MATCH (provider)-[:LIVES_IN_CITY]->(providerPrimaryCity:City)
OPTIONAL MATCH (providerAddress)-[:IS_LOCATED_IN_CITY]->(providerAddressCity:City)
OPTIONAL MATCH (providerPrimaryCity)-[:IS_PART_OF]->(providerPrimaryCityState:State)
OPTIONAL MATCH (providerAddressCity)-[:IS_PART_OF]->(providerAddressCityState:State)
OPTIONAL MATCH (provider)-[:BELONGS_TO]->(providerOrganization:Organization)

OPTIONAL MATCH (encounter)<-[:WAS_ORGANIZATION_FOR_ENCOUNTER]-(organization:Organization)
OPTIONAL MATCH (encounter)-[:BILLED_TO]->(payer:Payer)
OPTIONAL MATCH (payer)-[:HAS_ADDRESS]->(payerAddress:Address)
OPTIONAL MATCH (payer)-[:IS_LOCATED_IN_CITY]->(payerCity:City)
OPTIONAL MATCH (payerAddress)-[:IS_LOCATED_IN_CITY]->(addressCity:City)
OPTIONAL MATCH (payerCity)-[:IS_PART_OF]->(payerCityState:State)
OPTIONAL MATCH (addressCity)-[:IS_PART_OF]->(addressCityState:State)
OPTIONAL MATCH (payer)-[:IS_HEADQUARTERED_IN_STATE]->(headquartersState:State)

RETURN  
  patient, 
  encounter_class, 
  encounter_code, 
  diagnosis_code,
  patientCity, 
  patientCounty,
  patientState,
  patientGender,
  patientRace,
  patientEthnicity,
  patientAddress,
  patientAddressCity,
  
  provider AS provider,
  providerGender AS provider_gender,
  providerAddress AS provider_address,
  providerPrimaryCity AS provider_primary_city,
  providerAddressCity AS provider_address_city,
  providerPrimaryCityState AS provider_primary_city_state,
  providerAddressCityState AS provider_address_city_state,
  providerOrganization AS provider_organization,
  
  organization AS organization,
  payer AS payer,
  payerAddress AS payer_address,
  payerCity AS payer_city,
  addressCity AS address_city,
  payerCityState AS payer_city_state,
  addressCityState AS address_city_state,
  headquartersState AS headquarters_state
