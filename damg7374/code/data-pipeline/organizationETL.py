"""
ETL Code for loading Organization and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. Organization
2. City
3. State
4. Address

Relationships:
1. :Organization-[:HAS_ADDRESS]->[:Address]
2. :Organization-[:IS_LOCATED_IN_CITY]->[:City]
3. :City-[:IS_PART_OF]->[:State]
4. :Address-[:IS_LOCATED_IN_CITY]->:City

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
organizationsFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in organizationsFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in numeric columns with -999
numeric_columns = [field.name for field in organizationsFile_DF.schema.fields if isinstance(field.dataType, NumericType)]

logging.info("Preprocessing and cleaning the data")
organizationsFile_DF = (
    organizationsFile_DF
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .withColumnRenamed("ZIP", "ZIPCODE")
    .withColumnRenamed("LAT", "LATITUDE")
    .withColumnRenamed("LON", "LONGITUDE")
    .withColumn("STATE", mapping_expr[col("STATE")])
    .withColumn("currenttimestamp", F.current_timestamp())
)

# Iterate over each column, updating only those with string type
for field in organizationsFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        organizationsFile_DF = organizationsFile_DF.withColumn(field.name, upper(organizationsFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
organizationsFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
organizationsFile_DF.show()

logging.info("Inserting Organization Nodes")
#Organization Node
(
    organizationsFile_DF
    .select(
        "Id",
        "NAME",
        "PHONE",
        "REVENUE",
        "UTILIZATION",
        "currenttimestamp"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Organization")
    .option("node.keys", "Id")
    .save()
)

#Address Node
logging.info("Inserting Address Nodes")
(
    organizationsFile_DF
    .select(
        "ADDRESS",
        "ZIPCODE",
        "LATITUDE",
        "LONGITUDE"
    )
    .withColumnRenamed("ADDRESS", "NAME")
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Address")
    .option("node.keys", "NAME, ZIPCODE")
    .save()
)

#City Node
logging.info("Inserting City Nodes")
(
    organizationsFile_DF
    .select("CITY")
    .withColumnRenamed("CITY", "NAME")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":City")
    .option("node.keys", "NAME")
    .save()
)

#State Node
logging.info("Inserting State Nodes")
(
    organizationsFile_DF
    .select("STATE")
    .distinct()
    .withColumnRenamed("STATE", "NAME")
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":State")
    .option("node.keys", "NAME")
    .save()
)

#Relationship = :Organization-[:IS_LOCATED_IN_CITY]->:City
logging.info("Inserting Relationship (:Organization-[:IS_LOCATED_IN_CITY]->:City)")
(
    organizationsFile_DF
    .select(
        "Id",
        "CITY"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "IS_LOCATED_IN_CITY")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Organization")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":City")
    .option("relationship.target.node.keys", "CITY:NAME")

    .save()
)

#Relationship = :City-[:IS_PART_OF]->:State
logging.info("Inserting Relationship (:City-[:IS_PART_OF]->:State)")
(
    organizationsFile_DF
    .select(
        "CITY",
        "STATE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "IS_PART_OF")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":City")
    .option("relationship.source.node.keys", "CITY:NAME")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":State")
    .option("relationship.target.node.keys", "STATE:NAME")

    .save()
)

#Relationship = :Organization-[:HAS_ADDRESS]->:Address
logging.info("Inserting Relationship (:Organization-[:HAS_ADDRESS]->:Address)")
(
    organizationsFile_DF
    .select(
        "Id",
        "ADDRESS"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_ADDRESS")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Organization")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Address")
    .option("relationship.target.node.keys", "ADDRESS:NAME")

    .save()
)

#Relationship = :Address-[:IS_LOCATED_IN_CITY]->:City
(
    organizationsFile_DF
    .select(
        "ADDRESS",
        "ZIPCODE",
        "CITY"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "IS_LOCATED_IN_CITY")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Address")
    .option("relationship.source.node.keys", "ADDRESS:NAME, ZIPCODE")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":City")
    .option("relationship.target.node.keys", "CITY:NAME")

    .save()
)

logging.info("ETL Complete Successfully")
spark.stop()