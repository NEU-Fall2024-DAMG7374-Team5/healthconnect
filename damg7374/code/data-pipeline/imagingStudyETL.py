"""
ETL Code for loading Organization and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. SNOMEDCT
2. DICOMDCM
3. DICOMSOP
4. ImagingStudy

Relationships:
1. :Encounter-[:PERFORMED_IMAGING_STUDY]->:ImagingStudy
2. :ImagingStudy-[:HAS_PROCEDURE_CODE]->:SNOMEDCT
3. :ImagingStudy-[:HAS_BODYSITE_CODE]->:SNOMEDCT
4. :ImagingStudy-[:HAS_MODALITY_CODE]->:DICOMDCM
5. :ImagingStudy-[:HAS_SOP_CODE]->:DICOMSOP

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
    .appName("imagingStudyETL.py") \
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
imagingStudiesFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))
imagingStudiesFile_DF.printSchema()
imagingStudiesFile_DF.show(truncate=False)

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in imagingStudiesFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in numeric columns (skipping the amount columns) with -999
numeric_columns = [field.name for field in imagingStudiesFile_DF.schema.fields if isinstance(field.dataType, NumericType)]

imagingStudiesFile_DF = (
    imagingStudiesFile_DF
    # Fill missing values for string columns, numeric columns, and amount columns
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .withColumnRenamed("DATE", "IMAGING_STUDY_CONDUCTED_ON")
    .withColumn("PROCEDURE_DESCRIPTION", F.lit("UNKNOWN"))
)

#Replace nulls in date columns
date_columns = [field.name for field in imagingStudiesFile_DF.schema.fields if isinstance(field.dataType, TimestampType)]
default_date = '1900-01-01 00:00:00'

for d in date_columns:
    imagingStudiesFile_DF = imagingStudiesFile_DF.fillna({f'{d}':default_date})


# Iterate over each column, updating only those with string type
for field in imagingStudiesFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        imagingStudiesFile_DF = imagingStudiesFile_DF.withColumn(field.name, upper(imagingStudiesFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
imagingStudiesFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
imagingStudiesFile_DF.show()



#SNOMEDCT Node - Procedure
logging.info("Inserting SNOMEDCT - Procedure Nodes")

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
    imagingStudiesFile_DF
    .select(
        "PROCEDURE_CODE",
        "PROCEDURE_DESCRIPTION"
    )
    .distinct()
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("PROCEDURE_CODE", 'CODE')
    .withColumnRenamed("PROCEDURE_DESCRIPTION", 'DESCRIPTION')
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", cypher_query)
    .save()
)


#SNOMEDCT Node - BodySite
logging.info("Inserting SNOMEDCT - BodySite Nodes")

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
    imagingStudiesFile_DF
    .select(
        "BODYSITE_CODE",
        "BODYSITE_DESCRIPTION"
    )
    .distinct()
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("BODYSITE_CODE", 'CODE')
    .withColumnRenamed("BODYSITE_DESCRIPTION", 'DESCRIPTION')
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", cypher_query)
    .save()
)



#DICOMDCM Node
logging.info("Inserting DICOMDCM Nodes")

cypher_query = """

MERGE (n:DICOMDCM{CODE:event.CODE})
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
    imagingStudiesFile_DF
    .select(
        "MODALITY_CODE",
        "MODALITY_DESCRIPTION"
    )
    .distinct()
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("MODALITY_CODE", 'CODE')
    .withColumnRenamed("MODALITY_DESCRIPTION", 'DESCRIPTION')
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", cypher_query)
    .save()
)


#DICOMSOP Node
logging.info("Inserting DICOMSOP Nodes")
cypher_query = """

MERGE (n:DICOMSOP{CODE:event.CODE})
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
    imagingStudiesFile_DF
    .select(
        "SOP_CODE",
        "SOP_DESCRIPTION"
    )
    .distinct()
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("SOP_CODE", 'CODE')
    .withColumnRenamed("SOP_DESCRIPTION", 'DESCRIPTION')
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", cypher_query)
    .save()
)


#ImagingStudy Node
logging.info("Inserting ImagingStudy Nodes")
(
    imagingStudiesFile_DF
    .select(
        "Id",
        "IMAGING_STUDY_CONDUCTED_ON",
        "SERIES_UID",
        "INSTANCE_UID"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":ImagingStudy")
    .option("node.keys", "Id")
    .save()
)


#Relationship = :Encounter-[:PERFORMED_IMAGING_STUDY]->:ImagingStudy
logging.info("Inserting Relationship (:Encounter-[:PERFORMED_IMAGING_STUDY]->:ImagingStudy)")
(
    imagingStudiesFile_DF
    .select(
        "Id",
        "ENCOUNTER"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "PERFORMED_IMAGING_STUDY")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Encounter")
    .option("relationship.source.node.keys", "ENCOUNTER:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":ImagingStudy")
    .option("relationship.target.node.keys", "Id")

    .save()
)


#Relationship = :ImagingStudy-[:HAS_PROCEDURE_CODE]->:SNOMEDCT
logging.info("Inserting Relationship (:ImagingStudy-[:HAS_PROCEDURE_CODE]->:SNOMEDCT)")
(
    imagingStudiesFile_DF
    .select(
        "Id",
        "PROCEDURE_CODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_PROCEDURE_CODE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":ImagingStudy")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":SNOMEDCT")
    .option("relationship.target.node.keys", "PROCEDURE_CODE:CODE")

    .save()
)


#Relationship = :ImagingStudy-[:HAS_BODYSITE_CODE]->:SNOMEDCT
logging.info("Inserting Relationship (:ImagingStudy-[:HAS_BODYSITE_CODE]->:SNOMEDCT)")
(
    imagingStudiesFile_DF
    .select(
        "Id",
        "BODYSITE_CODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_BODYSITE_CODE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":ImagingStudy")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":SNOMEDCT")
    .option("relationship.target.node.keys", "BODYSITE_CODE:CODE")

    .save()
)


#Relationship = :ImagingStudy-[:HAS_MODALITY_CODE]->:DICOMDCM
logging.info("Inserting Relationship (:ImagingStudy-[:HAS_MODALITY_CODE]->:DICOMDCM)")
(
    imagingStudiesFile_DF
    .select(
        "Id",
        "MODALITY_CODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_MODALITY_CODE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":ImagingStudy")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":DICOMDCM")
    .option("relationship.target.node.keys", "MODALITY_CODE:CODE")

    .save()
)


#Relationship = :ImagingStudy-[:HAS_SOP_CODE]->:DICOMSOP
logging.info("Inserting Relationship (:ImagingStudy-[:HAS_SOP_CODE]->:DICOMSOP)")
(
    imagingStudiesFile_DF
    .select(
        "Id",
        "SOP_CODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_SOP_CODE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":ImagingStudy")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":DICOMSOP")
    .option("relationship.target.node.keys", "SOP_CODE:CODE")

    .save()
)


logging.info("ETL Complete Successfully")
spark.stop()