"""
ETL Code for loading Organization and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. Claim
2. SNOMEDCT

Relationships:
1. :Encounter-[:HAS_CLAIM]->:Claim
2. :Payer-[:IS_PAYER_FOR]->:Claim - PRIMARY PAYER
3. :Payer-[:IS_PAYER_FOR]->:Claim - SECONDARY PAYER
4. :Provider-[:WAS_PROVIDER_FOR]->:Claim - PRIMARY PROVIDER
5. :Provider-[:WAS_PROVIDER_FOR]->:Claim - REFERRING PROVIDER
6. :Provider-[:WAS_PROVIDER_FOR]->:Claim - SUPERVISING PROVIDER
7. :Claim-[:HAS_DIAGNOSIS_CODE]->:SNOMEDCT

"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import create_map, col, lit, upper
from pyspark.sql.types import StringType, NumericType, LongType
from itertools import chain
import logging
import re
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
    .appName("claimETL.py") \
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
claimsFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))
claimsFile_DF.printSchema()

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

#Identify Diagnosis Columns
diagnosis_columns = [col for col in claimsFile_DF.schema.names if re.match(r"DIAGNOSIS\d+$", col)]

claimsFile_DF = (
    claimsFile_DF
    
    # Convert the diagnosis columns to LongType if they are of StringType
    .select(
        *[
            F.col(col).cast(LongType()).alias(col) 
            if col in diagnosis_columns
            else F.col(col)
            for col in claimsFile_DF.columns
        ]
    )
)

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in claimsFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in amount columns with 0
amount_columns = ["OUTSTANDING1", "OUTSTANDING2", "OUTSTANDINGP"]

# Replace nulls in numeric columns (skipping the amount columns) with -999
numeric_columns = [field.name for field in claimsFile_DF.schema.fields if isinstance(field.dataType, NumericType)]
numeric_columns = [item for item in numeric_columns if item not in amount_columns]


F.col('DIAGNOSIS8').cast(LongType()).alias('DIAGNOSIS8')

claimsFile_DF = (
    claimsFile_DF
    # Fill missing values for string columns, numeric columns, and amount columns
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .fillna(0, subset=amount_columns)
)

# Iterate over each column, updating only those with string type
for field in claimsFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        claimsFile_DF = claimsFile_DF.withColumn(field.name, upper(claimsFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
claimsFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
claimsFile_DF.show()

logging.info("Inserting Claim Nodes")

#Claim Node
(
    claimsFile_DF
    .select(
        "Id",
        "DEPARTMENTID",
        "PATIENTDEPARTMENTID",
        "CURRENTILLNESSDATE",
        "SERVICEDATE",
        "STATUS1",
        "STATUS2",
        "STATUSP",
        "OUTSTANDING1",
        "OUTSTANDING2",
        "OUTSTANDINGP",
        "LASTBILLEDDATE1",
        "LASTBILLEDDATE2",
        "LASTBILLEDDATEP",
        "HEALTHCARECLAIMTYPEID1",
        "HEALTHCARECLAIMTYPEID2"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Claim")
    .option("node.keys", "Id")
    .save()
)


#Diagnosis Node

logging.info("Inserting Diagnosis Nodes")
num_columns = len(diagnosis_columns)
stack_expr = f"stack({num_columns}, {', '.join([f'`{col}`' for col in diagnosis_columns])}) as DIAGNOSIS"

cypher_query = """
MERGE (s:SNOMEDCT {CODE: event.CODE})
ON CREATE
SET 
    s.CODE = event.CODE,
    s.DESCRIPTION = event.DESCRIPTION
"""

(
    claimsFile_DF
    .selectExpr(stack_expr)
    .filter("DIAGNOSIS IS NOT NULL")
    .distinct()
    .withColumn("DESCRIPTION", F.lit("UNKNOWN"))
    .withColumnRenamed("DIAGNOSIS", "CODE")
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", cypher_query)
    .save()
)


#Relationship = :Encounter-[:HAS_CLAIM]->:Claim
logging.info("Inserting Relationship (:Encounter-[:HAS_CLAIM]->:Claim)")
(
    claimsFile_DF
    .select(
        "Id",
        "APPOINTMENTID"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_CLAIM")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Encounter")
    .option("relationship.source.node.keys", "APPOINTMENTID:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Claim")
    .option("relationship.target.node.keys", "Id")

    .save()
)


#Relationship = :Payer-[:IS_PAYER_FOR]->:Claim - PRIMARY PAYER
logging.info("Inserting Relationship (:Payer-[:IS_PAYER_FOR]->:Claim - PRIMARY PAYER)")
(
    claimsFile_DF
    .select(
        "Id",
        "PRIMARYPATIENTINSURANCEID"
    )
    .withColumn("TYPE", F.lit("PRIMARY"))
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "IS_PAYER_FOR")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "TYPE")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Payer")
    .option("relationship.source.node.keys", "PRIMARYPATIENTINSURANCEID:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Claim")
    .option("relationship.target.node.keys", "Id")


    .save()
)


#Relationship = :Payer-[:IS_PAYER_FOR]->:Claim - SECONDARY PAYER
logging.info("Inserting Relationship (:Payer-[:IS_PAYER_FOR]->:Claim - SECONDARY PAYER)")
(
    claimsFile_DF
    .select(
        "Id",
        "SECONDARYPATIENTINSURANCEID"
    )
    .withColumn("TYPE", F.lit("SECONDARY"))
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "IS_PAYER_FOR")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "TYPE")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Payer")
    .option("relationship.source.node.keys", "SECONDARYPATIENTINSURANCEID:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Claim")
    .option("relationship.target.node.keys", "Id")


    .save()
)


#Relationship = :Provider-[:WAS_PROVIDER_FOR]->:Claim - PRIMARY PROVIDER
logging.info("Inserting Relationship (:Provider-[:WAS_PROVIDER_FOR]->:Claim - PRIMARY PROVIDER)")
(
    claimsFile_DF
    .select(
        "Id",
        "PROVIDERID"
    )
    .withColumn("TYPE", F.lit("PRIMARY"))
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "WAS_PROVIDER_FOR")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "TYPE")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Provider")
    .option("relationship.source.node.keys", "PROVIDERID:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Claim")
    .option("relationship.target.node.keys", "Id")


    .save()
)

#Relationship = :Provider-[:WAS_PROVIDER_FOR]->:Claim - REFERRING PROVIDER
logging.info("Inserting Relationship (:Provider-[:WAS_PROVIDER_FOR]->:Claim - REFERRING PROVIDER)")
(
    claimsFile_DF
    .select(
        "Id",
        "REFERRINGPROVIDERID"
    )
    .withColumn("TYPE", F.lit("REFERRING"))
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "WAS_PROVIDER_FOR")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "TYPE")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Provider")
    .option("relationship.source.node.keys", "REFERRINGPROVIDERID:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Claim")
    .option("relationship.target.node.keys", "Id")


    .save()
)


#Relationship = :Provider-[:WAS_PROVIDER_FOR]->:Claim - SUPERVISING PROVIDER
logging.info("Inserting Relationship (:Provider-[:WAS_PROVIDER_FOR]->:Claim - SUPERVISING PROVIDER)")
(
    claimsFile_DF
    .select(
        "Id",
        "SUPERVISINGPROVIDERID"
    )
    .withColumn("TYPE", F.lit("SUPERVISING"))
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "WAS_PROVIDER_FOR")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "TYPE")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Provider")
    .option("relationship.source.node.keys", "SUPERVISINGPROVIDERID:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Claim")
    .option("relationship.target.node.keys", "Id")


    .save()
)


#Relationship = :Claim-[:HAS_DIAGNOSIS_CODE]->:SNOMEDCT
num_columns = len(diagnosis_columns)
stack_expr = ['Id', f"stack({num_columns}, {', '.join([f'`{col}`' for col in diagnosis_columns])}) as DIAGNOSIS"]

(

    claimsFile_DF
    .selectExpr(stack_expr)
    .filter("DIAGNOSIS != -999 and DIAGNOSIS IS NOT NULL")

    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_DIAGNOSIS_CODE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Claim")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":SNOMEDCT")
    .option("relationship.target.node.keys", "DIAGNOSIS:CODE")

    .save()
)

logging.info("ETL Complete Successfully")
spark.stop()