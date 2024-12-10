"""
ETL Code for loading Patient and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. Patient
2. Gender
3. Race
4. Ethnicity
5. Address
6. City
7. County
8. State

Relationships:
1. :Patient-[:HAS_RACE]->[:Race]
2. :Patient-[:HAS_ETHNICITY]->[:Ethnicity]
3. :Patient-[:HAS_GENDER]->[:Gender]
4. :Patient-[:HAS_ADDRESS]->[:Address]
5. :Patient-[:LIVES_IN_CITY]->[:City]
6. :City-[:IS_PART_OF]->[:County]
7. :County-[:IS_PART_OF]->[:State]
8. :City-[:IS_PART_OF]->[:State]
9. :Address-[:IS_LOCATED_IN_CITY]->:City

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
    "UNKNOWN":"UNKNOWN",
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
    .appName("patient_and_demographic_ETL.py") \
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
patientsFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))


mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in patientsFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in numeric columns with -999
numeric_columns = [field.name for field in patientsFile_DF.schema.fields if isinstance(field.dataType, NumericType)]

logging.info("Preprocessing and cleaning the data")
patientsFile_DF = (
    patientsFile_DF
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumn("STATE", upper(col('STATE')))
    .withColumn("GENDER",
            F.when(F.col("GENDER") == "F", "female")
            .when(F.col("GENDER") == "M", "male")
            .otherwise("Otherwise")
    )
    .withColumnRenamed("ZIP", "ZIPCODE")
    .withColumnRenamed("LAT", "LATITUDE")
    .withColumnRenamed("LON", "LONGITUDE")
)

# Iterate over each column, updating only those with string type
for field in patientsFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        patientsFile_DF = patientsFile_DF.withColumn(field.name, upper(patientsFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
patientsFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
patientsFile_DF.show()

logging.info("Inserting Patient Nodes")
#Patient Node
(
    patientsFile_DF
    .select(
        "Id",
        "BIRTHDATE",
        "DEATHDATE",
        "SSN",
        "DRIVERS",
        "PASSPORT",
        "PREFIX",
        "FIRST",
        "MIDDLE",
        "LAST",
        "SUFFIX",
        "MAIDEN",
        "MARITAL",
        "FIPS",
        "ZIPCODE",
        "LATITUDE",
        "LONGITUDE",
        "HEALTHCARE_EXPENSES",
        "HEALTHCARE_COVERAGE",
        "INCOME",
        "currenttimestamp"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Patient")
    .option("node.keys", "Id")
    .save()
)

logging.info("Inserting Race Nodes")
#Race Node
(
    patientsFile_DF
    .select("RACE")
    .withColumnRenamed("RACE", "NAME")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Race")
    .option("node.keys", "NAME")
    .save()
)

logging.info("Inserting Ethnicity Nodes")
#Ethnicity Node
(
    patientsFile_DF
    .select("ETHNICITY")
    .withColumnRenamed("ETHNICITY", "NAME")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Ethnicity")
    .option("node.keys", "NAME")
    .save()
)

logging.info("Inserting Gender Nodes")
#Gender Node
(
    patientsFile_DF
    .select("GENDER")
    .withColumnRenamed("GENDER", "NAME")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Gender")
    .option("node.keys", "NAME")
    .save()
)

logging.info("Inserting Address Nodes")
#Address Node
(
    patientsFile_DF
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

logging.info("Inserting City Nodes")
#City Node
(
    patientsFile_DF
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

logging.info("Inserting State Nodes")
#State Node
(
    patientsFile_DF
    .select("STATE")
    .withColumnRenamed("STATE", "NAME")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":State")
    .option("node.keys", "NAME")
    .save()
)

logging.info("Inserting County Nodes")
#County Node
(
    patientsFile_DF
    .select("COUNTY")
    .withColumnRenamed("COUNTY", "NAME")
    .distinct()
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":County")
    .option("node.keys", "NAME")
    .save()
)

logging.info("Inserting Relationship (:Patient-[:HAS_RACE]->[:Race])")
#Relationship = :Patient-[:HAS_RACE]->[:Race]
(
    patientsFile_DF
    .select(
        "Id",
        "RACE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_RACE")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Patient")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Race")
    .option("relationship.target.node.keys", "RACE:NAME")

    .save()
)

logging.info("Inserting Relationship (:Patient-[:HAS_ETHNICITY]->[:Ethnicity])")
#Relationship = :Patient-[:HAS_ETHNICITY]->[:Ethnicity]
(
    patientsFile_DF
    .select(
        "Id",
        "ETHNICITY"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_ETHNICITY")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Patient")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Ethnicity")
    .option("relationship.target.node.keys", "ETHNICITY:NAME")

    .save()
)

logging.info("Inserting Relationship (:Patient-[:HAS_GENDER]->[:Gender])")
#Relationship = :Patient-[:HAS_GENDER]->[:Gender]
(
    patientsFile_DF
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
    .option("relationship.source.labels", ":Patient")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Gender")
    .option("relationship.target.node.keys", "GENDER:NAME")

    .save()
)

logging.info("Inserting Relationship (:Patient-[:HAS_ADDRESS]->[:Address])")
#Relationship = :Patient-[:HAS_ADDRESS]->[:Address]
(
    patientsFile_DF
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
    .option("relationship.source.labels", ":Patient")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Address")
    .option("relationship.target.node.keys", "ADDRESS:NAME, ZIPCODE")

    .save()
)

logging.info("Inserting Relationship (:Patient-[:LIVES_IN_CITY]->[:City])")
#Relationship = :Patient-[:LIVES_IN_CITY]->[:City]
(
    patientsFile_DF
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
    .option("relationship.source.labels", ":Patient")
    .option("relationship.source.node.keys", "Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":City")
    .option("relationship.target.node.keys", "CITY:NAME")

    .save()
)

logging.info("Inserting Relationship (:City-[:IS_PART_OF]->[:County])")
#Relationship = :City-[:IS_PART_OF]->[:County]
(
    patientsFile_DF
    .select(
        "CITY",
        "COUNTY"
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
    .option("relationship.target.labels", ":County")
    .option("relationship.target.node.keys", "COUNTY:NAME")

    .save()
)

logging.info("Inserting Relationship (:County-[:IS_PART_OF]->[:State])")
#Relationship = :County-[:IS_PART_OF]->[:State]
(
    patientsFile_DF
    .select(
        "COUNTY",
        "STATE"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "IS_PART_OF")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":County")
    .option("relationship.source.node.keys", "COUNTY:NAME")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":State")
    .option("relationship.target.node.keys", "STATE:NAME")

    .save()
)

logging.info("Inserting Relationship (:City-[:IS_PART_OF]->[:State])")
#Relationship = :City-[:IS_PART_OF]->[:State]
(
    patientsFile_DF
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


#Relationship = :Address-[:IS_LOCATED_IN_CITY]->:City
(
    patientsFile_DF
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

