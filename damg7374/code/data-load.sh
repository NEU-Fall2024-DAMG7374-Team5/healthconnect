# Check if the properties file path is provided
if [ $# -eq 0 ]; then
    echo "Error: No properties file path provided."
    echo "Usage: $0 <AuraDBproperties | Localproperties | properties >"
    exit 1
fi



$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/patientETLProperties.properties \
./data-pipeline/patientETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/organizationETLProperties.properties \
./data-pipeline/organizationETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/providerETLProperties.properties \
./data-pipeline/providerETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/payerETLProperties.properties \
./data-pipeline/payerETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/encounterETLProperties.properties \
./data-pipeline/encounterETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/claimETLProperties.properties \
./data-pipeline/claimETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/conditionETLProperties.properties \
./data-pipeline/conditionETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/observationETLProperties.properties \
./data-pipeline/observationETL.py

$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/supplyETLProperties.properties \
./data-pipeline/supplyETL.py

$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/procedureETLProperties.properties \
./data-pipeline/procedureETL.py

$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/deviceETLProperties.properties \
./data-pipeline/deviceETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/immunizationETLProperties.properties \
./data-pipeline/immunizationETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/medicationETLProperties.properties \
./data-pipeline/medicationETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/allergyETLProperties.properties \
./data-pipeline/allergyETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/imagingStudyETLProperties.properties \
./data-pipeline/imagingStudyETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/careplanETLProperties.properties \
./data-pipeline/careplanETL.py



$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/payerTransitionETLProperties.properties \
./data-pipeline/payerTransitionETL.py


$SPARK_HOME/bin/spark-submit \
--master spark://Adityas-MacBook-Pro.local:7077 \
--jars ./utilities/neo4j-spark-connector.jar \
--properties-file ./$1/claimTransactionETLProperties.properties \
./data-pipeline/claimTransactionsETL.py
