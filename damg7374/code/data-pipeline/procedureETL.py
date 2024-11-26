"""
ETL Code for loading Organization and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. SNOMEDCT
2. Procedure

Relationships:
1. :Encounter-[:PERFORMED_PROCEDURE]->:Procedure
2. :Procedure-[:HAS_PROCEDURE_CODE]->:SNOMEDCT
3. :Procedure-[:IS_REASON_FOR_PROCEDURE]->:SNOMEDCT


"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import create_map, col, lit, upper
from pyspark.sql.types import StringType, NumericType, LongType
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
    .appName("procedureETL.py") \
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
proceduresFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))
proceduresFile_DF.printSchema()
proceduresFile_DF.show(truncate=False)

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in proceduresFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in amount columns with 0
amount_columns = ["BASE_COST"]

# Replace nulls in numeric columns (skipping the amount columns) with -999
numeric_columns = [field.name for field in proceduresFile_DF.schema.fields if isinstance(field.dataType, NumericType)]
numeric_columns = [item for item in numeric_columns if item not in amount_columns]


proceduresFile_DF = (
    proceduresFile_DF
    # Fill missing values for string columns, numeric columns, and amount columns
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .fillna(0, subset=amount_columns)
    .withColumnRenamed("START", "PROCEDURE_STARTED_ON")
    .withColumnRenamed("STOP", "PROCEDURE_STOPPED_ON")
    .withColumnRenamed("BASE_COST", "PROCEDURE_BASE_COST")
)

# Iterate over each column, updating only those with string type
for field in proceduresFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        proceduresFile_DF = proceduresFile_DF.withColumn(field.name, upper(proceduresFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
proceduresFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
proceduresFile_DF.show()


#SNOMEDCT Node
logging.info("Inserting SNOMEDCT Nodes - From Code")
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
    proceduresFile_DF
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
logging.info("Inserting SNOMEDCT Nodes - From ReasonCode")
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
    proceduresFile_DF
    .select(
        "REASONCODE",
        "REASONDESCRIPTION"
    )
    .withColumnRenamed("REASONCODE", "CODE")
    .withColumnRenamed("REASONDESCRIPTION", "DESCRIPTION")
    .distinct()
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", cypher_query)
    .save()
)

#Procedure Node
logging.info("Inserting Procedure Nodes")
(
    proceduresFile_DF
    .select(
        "PROCEDURE_STARTED_ON",
        "PROCEDURE_STOPPED_ON",
        "ENCOUNTER",
        "CODE",
        "PROCEDURE_BASE_COST"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Procedure")
    .option("node.keys", "PROCEDURE_STARTED_ON, PROCEDURE_STOPPED_ON, ENCOUNTER, CODE")
    .save()
)

#Relationship = :Encounter-[:PERFORMED_PROCEDURE]->:Procedure
logging.info("Inserting Relationship (:Encounter-[:PERFORMED_PROCEDURE]->:Procedure)")
(
    proceduresFile_DF
    .select(
        "PROCEDURE_STARTED_ON",
        "PROCEDURE_STOPPED_ON",
        "ENCOUNTER",
        "CODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "PERFORMED_PROCEDURE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Encounter")
    .option("relationship.source.node.keys", "ENCOUNTER:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Procedure")
    .option("relationship.target.node.keys", "PROCEDURE_STARTED_ON, PROCEDURE_STOPPED_ON, ENCOUNTER, CODE")

    .save()
)

#Relationship = :Procedure-[:HAS_PROCEDURE_CODE]->:SNOMEDCT
logging.info("Inserting Relationship (:Procedure-[:HAS_PROCEDURE_CODE]->:SNOMEDCT)")
(
    proceduresFile_DF
    .select(
        "PROCEDURE_STARTED_ON",
        "PROCEDURE_STOPPED_ON",
        "ENCOUNTER",
        "CODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_PROCEDURE_CODE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Procedure")
    .option("relationship.source.node.keys", "PROCEDURE_STARTED_ON, PROCEDURE_STOPPED_ON, ENCOUNTER, CODE")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":SNOMEDCT")
    .option("relationship.target.node.keys", "CODE")

    .save()
)

#Relationship = :Procedure-[:IS_REASON_FOR_PROCEDURE]->:SNOMEDCT
logging.info("Inserting Relationship (:Procedure-[:IS_REASON_FOR_PROCEDURE]->:SNOMEDCT)")
(
    proceduresFile_DF
    .select(
        "PROCEDURE_STARTED_ON",
        "PROCEDURE_STOPPED_ON",
        "ENCOUNTER",
        "CODE",
        "REASONCODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "IS_REASON_FOR_PROCEDURE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Procedure")
    .option("relationship.source.node.keys", "PROCEDURE_STARTED_ON, PROCEDURE_STOPPED_ON, ENCOUNTER, CODE")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":SNOMEDCT")
    .option("relationship.target.node.keys", "REASONCODE:CODE")

    .save()
)


logging.info("ETL Complete Successfully")
spark.stop()