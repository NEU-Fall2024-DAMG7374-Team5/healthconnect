"""
ETL Code for loading Provider and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. Provider
2. City
3. State
4. Address
5. Gender

Relationships:
1. :Provider-[:HAS_GENDER]->:Gender
2. :Provider-[:HAS_ADDRESS]->:Address
3. :Provider-[:LIVES_IN_CITY]->:City
4. :Address-[:IS_LOCATED_IN_CITY]->:City
5. :City-[:IS_PART_OF]->:State
6. :Provider-[:BELONGS_TO]->:Organization
7. :Address-[:IS_LOCATED_IN_CITY]->:City

"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import create_map, col, lit, upper
from itertools import chain
from pyspark.sql.types import StringType, NumericType
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
    .appName("provider_and_demographic_ETL.py") \
    .config('spark.executor.instances', 1) \
    .config('spark.executor.cores', 2) \
    .config('spark.executor.memory', '2G') \
    .config('spark.cores.max', 2)\
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
providersFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in providersFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in numeric columns with -999
numeric_columns = [field.name for field in providersFile_DF.schema.fields if isinstance(field.dataType, NumericType)]

providersFile_DF = (
    providersFile_DF
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .withColumn("STATE", mapping_expr[col("STATE")])
    .withColumn("GENDER",
        F.when(F.col("GENDER") == "F", "FEMALE")
        .when(F.col("GENDER") == "M", "MALE")
        .otherwise("OTHER")
    )
    .withColumnRenamed("ENCOUNTERS", "COUNT_OF_ENCOUNTERS_PERFORMED")
    .withColumnRenamed("PROCEDURES", "COUNT_OF_PROCEDURES_PERFORMED")
    .withColumnRenamed("ZIP", "ZIPCODE")
    .withColumnRenamed("LAT", "LATITUDE")
    .withColumnRenamed("LON", "LONGITUDE")
)

# Iterate over each column, updating only those with string type
for field in providersFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        providersFile_DF = providersFile_DF.withColumn(field.name, upper(providersFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
providersFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
providersFile_DF.show()

logging.info("Inserting Provider Nodes")

#Provider Node
(
    providersFile_DF
    .select(
    "Id",
    "NAME",
    "SPECIALITY",
    "COUNT_OF_ENCOUNTERS_PERFORMED",
    "COUNT_OF_PROCEDURES_PERFORMED"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Provider")
    .option("node.keys", "Id")
    .save()
)

#Gender Node
(
    providersFile_DF
    .select("GENDER")
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("GENDER", "NAME")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Gender")
    .option("node.keys", "NAME")
    .save()
)

#Address Node
(
    providersFile_DF
    .select(
        "ADDRESS",
        "ZIPCODE",
        "LATITUDE",
        "LONGITUDE"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("ADDRESS", "NAME")
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Address")
    .option("node.keys", "NAME, ZIPCODE")
    .save()
)

#City Node
(
    providersFile_DF
    .select("CITY")
    .withColumn("currenttimestamp", F.current_timestamp())
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
(
    providersFile_DF
    .select("STATE")
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("STATE", "NAME")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":State")
    .option("node.keys", "NAME")
    .save()
)

#Relationship = :Provider-[:HAS_GENDER]->:Gender
(
    providersFile_DF
    .select(
        "Id",
        "GENDER"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_GENDER")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Provider")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Gender")
    .option("relationship.target.node.keys", "GENDER:NAME")

    .save()
)

#Relationship = :Provider-[:HAS_ADDRESS]->:Address
(
    providersFile_DF
    .select(
        "Id",
        "ADDRESS",
        "ZIPCODE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_ADDRESS")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Provider")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Address")
    .option("relationship.target.node.keys", "ADDRESS:NAME, ZIPCODE:ZIPCODE")

    .save()
)

#Relationship = :Provider-[:LIVES_IN_CITY]->:City
(
    providersFile_DF
    .select(
        "Id",
        "CITY"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "LIVES_IN_CITY")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Provider")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":City")
    .option("relationship.target.node.keys", "CITY:NAME")

    .save()
)

#Relationship = :Address-[:IS_LOCATED_IN_CITY]->:City
(
    providersFile_DF
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

#Relationship = :City-[:IS_PART_OF]->:State
(
    providersFile_DF
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

#Relationship = :Provider-[:BELONGS_TO]->:Organization
(
    providersFile_DF
    .select(
        "Id",
        "ORGANIZATION"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "BELONGS_TO")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Provider")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Organization")
    .option("relationship.target.node.keys", "ORGANIZATION:Id")

    .save()
)

logging.info("ETL Complete Successfully")
spark.stop()