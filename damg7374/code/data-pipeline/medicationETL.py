"""
ETL Code for loading Organization and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. RXNORM
2. SNOMEDCT
3. Medication

Relationships:
1. :Encounter-[:PRESCRIBED_MEDICINE]->:Medication
2. :Medication-[:HAS_MEDICATION_CODE]->:RXNORM
3. :Medication-[:IS_REASON_FOR_PRESCRIPTION]->:SNOMEDCT
4. :Medication-[:COVERED_BY_PAYER]->:Payer

"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import create_map, col, lit, upper
from pyspark.sql.types import StringType, NumericType, LongType, TimestampType
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
    .appName("medicationETL.py") \
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
medicationsFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))
medicationsFile_DF.printSchema()
medicationsFile_DF.show(truncate=False)

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in medicationsFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in amount columns with 0
amount_columns = ["BASE_COST", "TOTALCOST", "PAYER_COVERAGE"]

# Replace nulls in numeric columns (skipping the amount columns) with -999
numeric_columns = [field.name for field in medicationsFile_DF.schema.fields if isinstance(field.dataType, NumericType)]
numeric_columns = [item for item in numeric_columns if item not in amount_columns]

medicationsFile_DF = (
    medicationsFile_DF
    # Fill missing values for string columns, numeric columns, and amount columns
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .fillna(0, subset=amount_columns)
    .withColumnRenamed("START", "MEDICATION_STARTED_ON")
    .withColumnRenamed("STOP", "MEDICATION_STOPPED_ON")
    .withColumnRenamed("BASE_COST", "MEDICATION_BASE_COST")
    .withColumnRenamed("TOTALCOST", "MEDICATION_TOTAL_COST")
    .withColumnRenamed("DISPENSES", "MEDICATION_DISPENSE_QUANTITY")
    .withColumnRenamed("PAYER_COVERAGE", "MEDICATION_PAYER_COVERAGE_AMOUNT")
)

#Replace nulls in date columns
date_columns = [field.name for field in medicationsFile_DF.schema.fields if isinstance(field.dataType, TimestampType)]
default_date = '1900-01-01 00:00:00'

for d in date_columns:
    medicationsFile_DF = medicationsFile_DF.fillna({f'{d}':default_date})


# Iterate over each column, updating only those with string type
for field in medicationsFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        medicationsFile_DF = medicationsFile_DF.withColumn(field.name, upper(medicationsFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
medicationsFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
medicationsFile_DF.show()


#RXNORM Node
logging.info("Inserting RXNORM Nodes")
cypher_query = """

MERGE (n:RXNORM{CODE:event.CODE})
ON CREATE
    SET 
    n.CODE = event.CODE,
    n.DESCRIPTION = event.DESCRIPTION
ON MATCH
    SET 
    n.DESCRIPTION =
    CASE
        WHEN event.DESCRIPTION <> 'UNKNOWN'
        THEN event.DESCRIPTION
        ELSE n.DESCRIPTION
    END
"""


(
    medicationsFile_DF
    .select(
        "CODE",
        "DESCRIPTION"
    )
    .distinct()
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", cypher_query)
    .save()
)


#SNOMEDCT Node
logging.info("Inserting SNOMEDCT Nodes")
cypher_query = """

MERGE (n:SNOMEDCT{CODE:event.CODE})
ON CREATE
    SET 
    n.CODE = event.CODE,
    n.DESCRIPTION = event.DESCRIPTION
ON MATCH
    SET 
    n.DESCRIPTION =
    CASE
        WHEN event.DESCRIPTION <> 'UNKNOWN'
        THEN event.DESCRIPTION
        ELSE n.DESCRIPTION
    END
"""


(
    medicationsFile_DF
    .select(
        "REASONCODE",
        "REASONDESCRIPTION"
    )
    .distinct()
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("REASONCODE", 'CODE')
    .withColumnRenamed("REASONDESCRIPTION", 'DESCRIPTION')
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", cypher_query)
    .save()
)



#Medication Node
logging.info("Inserting Medication Nodes")
(
    medicationsFile_DF
    .select(
        "MEDICATION_STARTED_ON",
        "MEDICATION_STOPPED_ON",
        "PAYER",
        "ENCOUNTER",
        "CODE", 
        "MEDICATION_BASE_COST",
        "MEDICATION_TOTAL_COST",
        "MEDICATION_DISPENSE_QUANTITY",
        
        
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Medication")
    .option("node.keys", "MEDICATION_STARTED_ON, MEDICATION_STOPPED_ON, PAYER, ENCOUNTER, CODE")
    .save()
)



#Relationship = :Encounter-[:PRESCRIBED_MEDICINE]->:Medication
logging.info("Inserting Relationship (:Encounter-[:PRESCRIBED_MEDICINE]->:Medication)")
(
    medicationsFile_DF
    .select(
        "ENCOUNTER",
        "PAYER",
        "CODE",
        "MEDICATION_STARTED_ON",
        "MEDICATION_STOPPED_ON"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "PRESCRIBED_MEDICINE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Encounter")
    .option("relationship.source.node.keys", "ENCOUNTER:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Medication")
    .option("relationship.target.node.keys", "MEDICATION_STARTED_ON, MEDICATION_STOPPED_ON, PAYER, ENCOUNTER, CODE")

    .save()
)


#Relationship = :Medication-[:HAS_MEDICATION_CODE]->:RXNORM
logging.info("Inserting Relationship (:Medication-[:HAS_MEDICATION_CODE]->:RXNORM)")
(
    medicationsFile_DF
    .select(
        "ENCOUNTER",
        "PAYER",
        "CODE",
        "MEDICATION_STARTED_ON",
        "MEDICATION_STOPPED_ON"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_MEDICATION_CODE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Medication")
    .option("relationship.source.node.keys", "MEDICATION_STARTED_ON, MEDICATION_STOPPED_ON, PAYER, ENCOUNTER, CODE")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":RXNORM")
    .option("relationship.target.node.keys", "CODE")

    .save()
)


#Relationship = :Medication-[:IS_REASON_FOR_PRESCRIPTION]->:SNOMEDCT
logging.info("Inserting Relationship (:Medication-[:IS_REASON_FOR_PRESCRIPTION]->:SNOMEDCT)")
(
    medicationsFile_DF
    .select(
        "ENCOUNTER",
        "PAYER",
        "CODE",
        "MEDICATION_STARTED_ON",
        "MEDICATION_STOPPED_ON",
        "REASONCODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "IS_REASON_FOR_PRESCRIPTION")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Medication")
    .option("relationship.source.node.keys", "MEDICATION_STARTED_ON, MEDICATION_STOPPED_ON, PAYER, ENCOUNTER, CODE")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":SNOMEDCT")
    .option("relationship.target.node.keys", "REASONCODE:CODE")

    .save()
)


#Relationship = :Medication-[:COVERED_BY_PAYER]->:Payer
logging.info("Inserting Relationship (:Medication-[:COVERED_BY_PAYER]->:Payer)")
(
    medicationsFile_DF
    .select(
        "ENCOUNTER",
        "PAYER",
        "CODE",
        "MEDICATION_STARTED_ON",
        "MEDICATION_STOPPED_ON",
        "MEDICATION_PAYER_COVERAGE_AMOUNT"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "COVERED_BY_PAYER")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "MEDICATION_PAYER_COVERAGE_AMOUNT")


    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Medication")
    .option("relationship.source.node.keys", "MEDICATION_STARTED_ON, MEDICATION_STOPPED_ON, PAYER, ENCOUNTER, CODE")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Payer")
    .option("relationship.target.node.keys", "PAYER:Id")

    .save()
)


logging.info("ETL Complete Successfully")
spark.stop()