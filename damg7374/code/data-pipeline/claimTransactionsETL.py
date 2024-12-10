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
    .appName("claimTransactionsETL.py") \
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
claimsTransactionsFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))
claimsTransactionsFile_DF.printSchema()
claimsTransactionsFile_DF.show(truncate=False)

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data

#Not replacing the columns with either -999 or UKNOWN, the claims transactions data follows one big table format, so any null values
#represent they aren't applicable. The neo4j connector will skip creating those in KG.
claimsTransactionsFile_DF.withColumnRenamed("ID", "Id")

# Iterate over each column, updating only those with string type
for field in claimsTransactionsFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        claimsTransactionsFile_DF = claimsTransactionsFile_DF.withColumn(field.name, upper(claimsTransactionsFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
claimsTransactionsFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
claimsTransactionsFile_DF.show()

#ClaimTransaction Node
logging.info("Inserting ClaimTransaction Nodes")
(
    claimsTransactionsFile_DF
    .select(
        "Id",
        "CLAIMID",
        "CHARGEID",
        # "PATIENTID",
        "TYPE",
        "AMOUNT",
        "METHOD",
        "FROMDATE",
        "TODATE",
        "PLACEOFSERVICE",
        "PROCEDURECODE",
        # "MODIFIER1",
        # "MODIFIER2",
        
        # "DIAGNOSISREF1",
        # "DIAGNOSISREF2",
        # "DIAGNOSISREF3",
        # "DIAGNOSISREF4",
        "UNITS",
        "DEPARTMENTID",
        "NOTES",
        "UNITAMOUNT",
        "TRANSFEROUTID",
        "TRANSFERTYPE",
        "PAYMENTS",
        "ADJUSTMENTS",
        "TRANSFERS",
        "OUTSTANDING",
        # "APPOINTMENTID",
        "LINENOTE",
        # "PATIENTINSURANCEID",
        "FEESCHEDULEID",
        # "PROVIDERID",
        # "SUPERVISINGPROVIDERID"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":ClaimTransaction")
    .option("node.keys", "Id")
    .save()
)


#Relationship = :Claim-[:HAS_TRANSACTION]->:ClaimTransaction
logging.info("Inserting Relationship (:Claim-[:HAS_TRANSACTION]->:ClaimTransaction)")
(
    claimsTransactionsFile_DF
    .select(
        "Id",
        "CLAIMID"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_TRANSACTION")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Claim")
    .option("relationship.source.node.keys", "CLAIMID:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", "ClaimTransaction")
    .option("relationship.target.node.keys", "Id")

    .save()
)


logging.info("ETL Complete Successfully")
spark.stop()