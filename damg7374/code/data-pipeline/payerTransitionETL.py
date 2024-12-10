"""
ETL Code for loading Organization and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. SNOMEDCT

Relationships:
1. :Encounter-[:USED_SUPPLY]->:SNOMEDCT

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
    .appName("payerTransitionETL.py") \
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
payerTransitionsFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))
payerTransitionsFile_DF.printSchema()
payerTransitionsFile_DF.show(truncate=False)

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in payerTransitionsFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in numeric columns (skipping the amount columns) with -999
numeric_columns = [field.name for field in payerTransitionsFile_DF.schema.fields if isinstance(field.dataType, NumericType)]

payerTransitionsFile_DF = (
    payerTransitionsFile_DF
    # Fill missing values for string columns, numeric columns, and amount columns
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .withColumnRenamed("START_DATE", "MEMBERSHIP_START_DATE")
    .withColumnRenamed("END_DATE", "MEMBERSHIP_END_DATE")
)

#Replace nulls in date columns
date_columns = [field.name for field in payerTransitionsFile_DF.schema.fields if isinstance(field.dataType, TimestampType)]
default_date = '1900-01-01 00:00:00'

for d in date_columns:
    payerTransitionsFile_DF = payerTransitionsFile_DF.fillna({f'{d}':default_date})


# Iterate over each column, updating only those with string type
for field in payerTransitionsFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        payerTransitionsFile_DF = payerTransitionsFile_DF.withColumn(field.name, upper(payerTransitionsFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
payerTransitionsFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
payerTransitionsFile_DF.show()



#Relationship = :Patient-[:HAS_PAYER]->:Payer
logging.info("Inserting Relationship (:Patient-[:HAS_PAYER]->:Payer)")
(
    payerTransitionsFile_DF
    .select(
        "PATIENT",
        "MEMBERID",
        "MEMBERSHIP_START_DATE",
        "MEMBERSHIP_END_DATE",
        "PAYER",
        "PLAN_OWNERSHIP",
        "OWNER_NAME"
    )
    .withColumn("TYPE", F.lit("PRIMARY"))
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_PAYER")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "MEMBERID, MEMBERSHIP_START_DATE, MEMBERSHIP_END_DATE, PLAN_OWNERSHIP, OWNER_NAME, TYPE")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Patient")
    .option("relationship.source.node.keys", "PATIENT:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Payer")
    .option("relationship.target.node.keys", "PAYER:Id")

    .save()
)


#Relationship = :Patient-[:HAS_PAYER]->:Payer
logging.info("Inserting Relationship (:Patient-[:HAS_PAYER]->:Payer)")
(
    payerTransitionsFile_DF
    .select(
        "PATIENT",
        "MEMBERSHIP_START_DATE",
        "MEMBERSHIP_END_DATE",
        "SECONDARY_PAYER"
    )
    .withColumn("TYPE", F.lit("SECONDARY"))
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_PAYER")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "MEMBERSHIP_START_DATE, MEMBERSHIP_END_DATE, TYPE")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Patient")
    .option("relationship.source.node.keys", "PATIENT:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Payer")
    .option("relationship.target.node.keys", "SECONDARY_PAYER:Id")

    .save()
)


#Relationship = :Payer-[:HAS_CUSTOMER]->:Patient
logging.info("Inserting Relationship (:Payer-[:HAS_CUSTOMER]->:Patient)")
(
    payerTransitionsFile_DF
    .select(
        "PATIENT",
        "MEMBERID",
        "MEMBERSHIP_START_DATE",
        "MEMBERSHIP_END_DATE",
        "PAYER",
        "PLAN_OWNERSHIP",
        "OWNER_NAME"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_CUSTOMER")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "MEMBERID, MEMBERSHIP_START_DATE, MEMBERSHIP_END_DATE, PLAN_OWNERSHIP, OWNER_NAME")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Payer")
    .option("relationship.source.node.keys", "PAYER:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Patient")
    .option("relationship.target.node.keys", "PATIENT:Id")

    .save()
)


#Relationship = :Payer-[:HAS_CUSTOMER]->:Patient
logging.info("Inserting Relationship (:Payer-[:HAS_CUSTOMER]->:Patient)")
(
    payerTransitionsFile_DF
    .select(
        "PATIENT",
        "MEMBERSHIP_START_DATE",
        "MEMBERSHIP_END_DATE",
        "SECONDARY_PAYER"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_CUSTOMER")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "MEMBERSHIP_START_DATE, MEMBERSHIP_END_DATE")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Payer")
    .option("relationship.source.node.keys", "SECONDARY_PAYER:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Patient")
    .option("relationship.target.node.keys", "PATIENT:Id")

    .save()
)


logging.info("ETL Complete Successfully")
spark.stop()