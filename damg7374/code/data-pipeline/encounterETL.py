"""
ETL Code for loading Organization and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. Encounter
2. EncounterClass
3. SNOMEDCT

Relationships:
1. :Encounter-[:HAS_ENCOUNTER_CLASS]->:EncounterClass
2. :Encounter-[:HAS_ENCOUNTER_CODE]->:SNOMEDCT
3. :Encounter-[:HAS_DIAGNOSIS_CODE]->:SNOMEDCT
4. :Patient-[:HAD_ENCOUNTER]->:Encounter
5. :Provider-[:WAS_PROVIDER_FOR_ENCOUNTER]->:Encounter
6. :Organization-[:WAS_ORGANIZATION_FOR_ENCOUNTER]->:Encounter
7. :Encounter-[:BILLED_TO]->:Payer

"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import create_map, col, lit, upper
from pyspark.sql.types import StringType, NumericType
from itertools import chain
import logging
logging.basicConfig(level=logging.INFO)

#Lookup Variables
us_states = {
    "AL": "ALABAMA",
    "AK": "ALASKA",
    "AZ": "ARIZONA",
    "AR": "ARKANSAS",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "GA": "GEORGIA",
    "HI": "HAWAII",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "IA": "IOWA",
    "KS": "KANSAS",
    "KY": "KENTUCKY",
    "LA": "LOUISIANA",
    "ME": "MAINE",
    "MD": "MARYLAND",
    "MA": "MASSACHUSETTS",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MS": "MISSISSIPPI",
    "MO": "MISSOURI",
    "MT": "MONTANA",
    "NE": "NEBRASKA",
    "NV": "NEVADA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NY": "NEW YORK",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UT": "UTAH",
    "VT": "VERMONT",
    "VA": "VIRGINIA",
    "WA": "WASHINGTON",
    "WV": "WEST VIRGINIA",
    "WI": "WISCONSIN",
    "WY": "WYOMING"
}


def setup_neo4j_configs(spark):
    """Map spark.neo4j.* configs to neo4j.* configs"""
    conf = spark.sparkContext.getConf()
    
    # Get configs with 'spark.neo4j.' prefix
    url = conf.get('spark.neo4j.url')
    username = conf.get('spark.neo4j.authentication.basic.username')
    password = conf.get('spark.neo4j.authentication.basic.password')
    database = conf.get('spark.neo4j.database')
    
    # Set configs without 'spark.' prefix
    spark.conf.set('neo4j.url', url)
    spark.conf.set('neo4j.authentication.basic.username', username)
    spark.conf.set('neo4j.authentication.basic.password', password)
    spark.conf.set('neo4j.database', database)


print("Connecting to Spark Cluster with configurations")
spark = SparkSession.builder \
    .appName("organization_and_demographic_ETL.py") \
    .config('spark.executor.instances', 1)\
    .config('spark.executor.cores', 2) \
    .config('spark.executor.memory', '2G') \
    .config('spark.cores.max', 2) \
    .getOrCreate()

#Get additional Airflow Sent Config relating to Source Dataset
SOURCE_DATASET_LOCATION = spark.sparkContext.getConf().get('spark.source.dataset.location')
SOURCE_DATASET_FILENAME = spark.sparkContext.getConf().get('spark.source.dataset.filename')
TRIGGER_PARENT = spark.sparkContext.getConf().get('spark.trigger.parent')

#Get additional Airflow Sent Config relating to Target Database
#Neo4j Config sent by Airflow have spark. prefix which makes it incompatible with Apache Spark Neo4j connector config expectations
#Renaming is needed
setup_neo4j_configs(spark)




# Read Source CSV file
logging.info(f"Trigger Parent: {TRIGGER_PARENT}")
logging.info("Attempting to read Source CSV file")
encountersFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in encountersFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in numeric columns with -999
numeric_columns = [field.name for field in encountersFile_DF.schema.fields if isinstance(field.dataType, NumericType)]

logging.info("Preprocessing and cleaning the data")
encountersFile_DF = (
    encountersFile_DF
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .withColumnRenamed("CODE", "ENCOUNTER_CODE")
    .withColumnRenamed("DESCRIPTION", "ENCOUNTER_DESCRIPTION")
    .withColumnRenamed("REASONCODE", "DIAGNOSIS_CODE")
    .withColumnRenamed("REASONDESCRIPTION", "DIAGNOSIS_DESCRIPTION")
)


# Iterate over each column, updating only those with string type
for field in encountersFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        encountersFile_DF = encountersFile_DF.withColumn(field.name, upper(encountersFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
encountersFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
encountersFile_DF.show()

logging.info("Inserting Encounter Nodes")
#Encounter Node
(
    encountersFile_DF
    .select(
        "Id",
        "START",
        "STOP",
        "BASE_ENCOUNTER_COST",
        "TOTAL_CLAIM_COST",
        "PAYER_COVERAGE"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Encounter")
    .option("node.keys", "Id")
    .save()
)

logging.info("Inserting EncounterClass Nodes")
#EncounterClass Node
(
    encountersFile_DF
    .select(
        "ENCOUNTERCLASS"
    )
    .distinct()
    .withColumnRenamed("ENCOUNTERCLASS", "NAME")
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":EncounterClass")
    .option("node.keys", "NAME")
    .save()
)


logging.info("Inserting Encounter - SNOMEDCT Nodes")
#Encounter - SNOMEDCT Node
(
    encountersFile_DF
    .select(
        "ENCOUNTER_CODE",
        "ENCOUNTER_DESCRIPTION"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("ENCOUNTER_CODE", "CODE")
    .withColumnRenamed("ENCOUNTER_DESCRIPTION", "DESCRIPTION")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":SNOMEDCT")
    .option("node.keys", "CODE")
    .save()
)


logging.info("Inserting Diagnosis - SNOMEDCT Nodes")
#Encounter Diagnosis - SNOMEDCT Node
(
    encountersFile_DF
    .select(
        "DIAGNOSIS_CODE",
        "DIAGNOSIS_DESCRIPTION"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("DIAGNOSIS_CODE", "CODE")
    .withColumnRenamed("DIAGNOSIS_DESCRIPTION", "DESCRIPTION")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":SNOMEDCT")
    .option("node.keys", "CODE")
    .save()
)

logging.info("Inserting Relationship (:Encounter-[:HAS_ENCOUNTER_CLASS]->:EncounterClass)")
#Relationship = :Encounter-[:HAS_ENCOUNTER_CLASS]->:EncounterClass
(
    encountersFile_DF
    .select(
        "Id",
        "ENCOUNTERCLASS"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_ENCOUNTER_CLASS")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Encounter")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":EncounterClass")
    .option("relationship.target.node.keys", "ENCOUNTERCLASS:NAME")

    .save()
)


logging.info("Inserting Relationship (:Encounter-[:HAS_ENCOUNTER_CODE]->:SNOMEDCT)")
#Relationship = :Encounter-[:HAS_ENCOUNTER_CODE]->:SNOMEDCT
(
    encountersFile_DF
    .select(
        "Id",
        "ENCOUNTER_CODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_ENCOUNTER_CODE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Encounter")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":SNOMEDCT")
    .option("relationship.target.node.keys", "ENCOUNTER_CODE:CODE")

    .save()
)


logging.info("Inserting Relationship (:Encounter-[:HAS_DIAGNOSIS_CODE]->:SNOMEDCT)")
#Relationship = :Encounter-[:HAS_DIAGNOSIS_CODE]->:SNOMEDCT
(
    encountersFile_DF
    .select(
        "Id",
        "DIAGNOSIS_CODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_DIAGNOSIS_CODE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Encounter")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":SNOMEDCT")
    .option("relationship.target.node.keys", "DIAGNOSIS_CODE:CODE")

    .save()
)

logging.info("Inserting Relationship (:Patient-[:HAD_ENCOUNTER]->:Encounter)")
#Relationship = :Patient-[:HAD_ENCOUNTER]->:Encounter
(
    encountersFile_DF
    .select(
        "Id",
        "PATIENT"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAD_ENCOUNTER")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Patient")
    .option("relationship.source.node.keys", "PATIENT:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Encounter")
    .option("relationship.target.node.keys", "Id")

    .save()
)

logging.info("Inserting Relationship (:Provider-[:WAS_PROVIDER_FOR_ENCOUNTER]->:Encounter)")
#Relationship = :Provider-[:WAS_PROVIDER_FOR_ENCOUNTER]->:Encounter
(
    encountersFile_DF
    .select(
        "Id",
        "PROVIDER"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "WAS_PROVIDER_FOR_ENCOUNTER")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Provider")
    .option("relationship.source.node.keys", "PROVIDER:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Encounter")
    .option("relationship.target.node.keys", "Id")

    .save()
)


logging.info("Inserting Relationship (:Organization-[:WAS_ORGANIZATION_FOR_ENCOUNTER]->:Encounter)")
#Relationship = :Organization-[:WAS_ORGANIZATION_FOR_ENCOUNTER]->:Encounter
(
    encountersFile_DF
    .select(
        "Id",
        "ORGANIZATION"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "WAS_ORGANIZATION_FOR_ENCOUNTER")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Organization")
    .option("relationship.source.node.keys", "ORGANIZATION:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Encounter")
    .option("relationship.target.node.keys", "Id")

    .save()
)


logging.info("Inserting Relationship (:Encounter-[:BILLED_TO]->:Payer)")
#Relationship = :Encounter-[:BILLED_TO]->:Payer
(
    encountersFile_DF
    .select(
        "Id",
        "PAYER"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "BILLED_TO")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Encounter")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Payer")
    .option("relationship.target.node.keys", "PAYER:Id")

    .save()
)


logging.info("ETL Complete Successfully")
spark.stop()