"""
ETL Code for loading Organization and Demographic Details into HealthConnect Knowledge Graph

Nodes:
1. SNOMEDCT
2. RXNORM
3. ALLERGY

Relationships:
1. :Encounter-[:RECORDED_ALLERGY]->:Allergy
2. :Allergy-[:HAS_ALLERGY_CODE]->:SNOMEDCT|RXNORM
3. :Allergy-[:HAS_REACTION_CODE]->:SNOMEDCT

"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import create_map, col, lit, upper, to_timestamp
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
    .appName("allergyETL.py") \
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
allergiesFile_DF = (spark.read.csv(f"{SOURCE_DATASET_LOCATION}/{SOURCE_DATASET_FILENAME}", header=True, inferSchema=True))
allergiesFile_DF.printSchema()
allergiesFile_DF.show(truncate=False)

mapping_expr = create_map([lit(x) for x in chain(*us_states.items())])

#Preprocessing of data
allergiesFile_DF = allergiesFile_DF.withColumn("STOP", to_timestamp(allergiesFile_DF["STOP"], "yyyy-MM-dd HH:mm:ss"))

# Replace nulls in string columns with 'UNKNOWN'
string_columns = [field.name for field in allergiesFile_DF.schema.fields if isinstance(field.dataType, StringType)]

# Replace nulls in numeric columns (skipping the amount columns) with -999
numeric_columns = [field.name for field in allergiesFile_DF.schema.fields if isinstance(field.dataType, NumericType)]

allergiesFile_DF = (
    allergiesFile_DF
    # Fill missing values for string columns, numeric columns, and amount columns
    .fillna("UNKNOWN", subset=string_columns)
    .fillna(-999, subset=numeric_columns)
    .withColumnRenamed("START", "ALLERGY_STARTED_ON")
    .withColumnRenamed("STOP", "ALLERGY_STOPPED_ON")
    .replace({'Unknown':'SNOMEDCT'}, subset=['SYSTEM'])
)

#Replace nulls in date columns
date_columns = [field.name for field in allergiesFile_DF.schema.fields if isinstance(field.dataType, TimestampType)]
default_date = '1900-01-01 00:00:00'

for d in date_columns:
    allergiesFile_DF = allergiesFile_DF.fillna({f'{d}':default_date})


# Iterate over each column, updating only those with string type
for field in allergiesFile_DF.schema.fields:
    if field.dataType.simpleString() == 'string':
        allergiesFile_DF = allergiesFile_DF.withColumn(field.name, upper(allergiesFile_DF[field.name]))

logging.info("Source Dataset Schema after data cleaning")
allergiesFile_DF.printSchema()

logging.info("Source Dataset Schema Sample Records")
allergiesFile_DF.show()



#RXNORM | SNOMEDCT Node
logging.info("Inserting RXNORM | SNOMEDCT Nodes")

# Get distinct SYSTEM values
systems = allergiesFile_DF.select("SYSTEM").distinct().collect()

# Base Cypher query template with a placeholder for the dynamic label
cypher_query_template = """
MERGE (n:{label} {CODE: event.CODE})
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

# Iterate over each distinct SYSTEM value
for row in systems:

    system_value = row["SYSTEM"]
    logging.info(f"Inserting {system_value} Nodes")

    # Dynamically build the Cypher query with the correct label
    cypher_query = cypher_query_template.replace("{label}", system_value)
    
    # Filter the DataFrame for the current SYSTEM value
    filtered_df = allergiesFile_DF.filter(F.col("SYSTEM") == system_value)
    
    # Write the filtered data to Neo4j with the dynamic label in the Cypher query
    (
        filtered_df
        .select("CODE", "DESCRIPTION")  # Select relevant columns
        .distinct()
        .withColumn("currenttimestamp", F.current_timestamp())
        .write
        .format("org.neo4j.spark.DataSource")
        .mode("Append")
        .option("query", cypher_query)  # Pass the dynamically generated Cypher query
        .save()
    )


#Allergy Node
logging.info(f"Inserting Allergy Nodes")
(
    allergiesFile_DF
    .select(
        "ALLERGY_STARTED_ON",
        "ALLERGY_STOPPED_ON",
        "ENCOUNTER",
        "CODE",
        "SYSTEM"
    )
    .withColumn("currenttimestamp", F.current_timestamp())
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", ":Allergy")
    .option("node.keys", "ALLERGY_STARTED_ON, ALLERGY_STOPPED_ON, ENCOUNTER, CODE, SYSTEM")
    .save()
)


#Normalizing the denormalised data across columns - REACTION, SEVERITY, DESCRIPTION

def normalize_allergy_data(df, num_reactions):
    # Start with an empty list to hold individual DataFrames for each reaction set
    unioned_df = None
    
    # Iterate over the number of reactions (e.g., 1, 2, 3, etc.)
    for i in range(1, num_reactions + 1):
        # Create column names dynamically for REACTION, DESCRIPTION, and SEVERITY
        reaction_col = f"REACTION{i}"
        description_col = f"DESCRIPTION{i}"
        severity_col = f"SEVERITY{i}"
        
        # Create a new DataFrame for each reaction set
        temp_df = df.select(
            'ALLERGY_STARTED_ON', 
            'ALLERGY_STOPPED_ON', 
            'ENCOUNTER', 
            'CODE', 
            'SYSTEM', 
            'TYPE', 
            'CATEGORY',
            F.col(reaction_col).alias('REACTION'),
            F.col(description_col).alias('REACTION_DESCRIPTION'),
            F.col(severity_col).alias('REACTION_SEVERITY')
        )
        
        # # Remove rows where the reaction is null (optional: you can filter other columns as needed)
        temp_df = temp_df.filter("REACTION != -999")
        
        # Union the current DataFrame with the previous ones (if any)
        if unioned_df is None:
            unioned_df = temp_df
        else:
            unioned_df = unioned_df.union(temp_df)
    
    # Return the final unioned DataFrame
    return unioned_df

logging.info(f"Normalising the dataframe to get reactions")
# Example usage: assuming allergiesFile_DF is your DataFrame and you want to normalize 2 reaction sets
normalized_df = normalize_allergy_data(allergiesFile_DF, 2)


#SNOMEDCT Node - REACTION
logging.info(f"Inserting SNOMEDCT Node - REACTION Nodes")
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
    normalized_df
    .select(
        "REACTION",
        "REACTION_DESCRIPTION"
    )
    .distinct()
    .withColumn("currenttimestamp", F.current_timestamp())
    .withColumnRenamed("REACTION", 'CODE')
    .withColumnRenamed("REACTION_DESCRIPTION", 'DESCRIPTION')
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", cypher_query)
    .save()
)


#Relationship = :Encounter-[:RECORDED_ALLERGY]->:Allergy
logging.info("Inserting Relationship (:Encounter-[:RECORDED_ALLERGY]->:Allergy)")
(
    allergiesFile_DF
    .select(
        "ENCOUNTER",
        "CODE",
        "SYSTEM",
        "ALLERGY_STARTED_ON",
        "ALLERGY_STOPPED_ON"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "RECORDED_ALLERGY")
    .option("relationship.save.strategy", "keys")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Encounter")
    .option("relationship.source.node.keys", "ENCOUNTER:Id")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":Allergy")
    .option("relationship.target.node.keys", "ALLERGY_STARTED_ON, ALLERGY_STOPPED_ON, ENCOUNTER, SYSTEM,CODE")

    .save()
)


#Relationship = :Allergy-[:HAS_ALLERGY_CODE]->:SNOMEDCT|RXNORM
logging.info("Inserting Relationship (:Allergy-[:HAS_ALLERGY_CODE]->:SNOMEDCT|RXNORM)")


# Collect distinct SYSTEM values from the DataFrame
system_values = allergiesFile_DF.select("SYSTEM").distinct().collect()

# Iterate over the distinct SYSTEM values and write the data for each
for row in system_values:
    system_value = row["SYSTEM"]
    
    # Filter the DataFrame based on the current SYSTEM value
    df_system = allergiesFile_DF.filter(F.col("SYSTEM") == system_value)
    
    # Write to Neo4j with the dynamic target label based on the SYSTEM value
    (
        df_system
        .select(
            "ENCOUNTER",
            "CODE",
            "SYSTEM",
            "ALLERGY_STARTED_ON",
            "ALLERGY_STOPPED_ON"
        )
        .write
        .format("org.neo4j.spark.DataSource")
        .mode("Overwrite")
        .option("relationship", "HAS_ALLERGY_CODE")
        .option("relationship.save.strategy", "keys")

        # Source Node configuration
        .option("relationship.source.save.mode", "Match")
        .option("relationship.source.labels", ":Allergy")
        .option("relationship.source.node.keys", "ALLERGY_STARTED_ON, ALLERGY_STOPPED_ON, ENCOUNTER, SYSTEM, CODE")

        # Target Node configuration with dynamic label based on SYSTEM
        .option("relationship.target.save.mode", "Match")
        .option("relationship.target.labels", f":{system_value}")  # Dynamic label
        .option("relationship.target.node.keys", "CODE")
        
        .save()
    )


#Relationship = :Allergy-[:HAS_REACTION_CODE]->:SNOMEDCT
logging.info("Inserting Relationship (:Allergy-[:HAS_REACTION_CODE]->:SNOMEDCT)")
(
    normalized_df
    .select(
        "ENCOUNTER",
        "CODE",
        "SYSTEM",
        "ALLERGY_STARTED_ON",
        "ALLERGY_STOPPED_ON",
        "REACTION",
        "REACTION_SEVERITY"
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("relationship", "HAS_REACTION_CODE")
    .option("relationship.save.strategy", "keys")
    .option("relationship.properties", "REACTION_SEVERITY")

    #Source
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.labels", ":Allergy")
    .option("relationship.source.node.keys", "ALLERGY_STARTED_ON, ALLERGY_STOPPED_ON, ENCOUNTER, SYSTEM,CODE")
    

    #Target
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.labels", ":SNOMEDCT")
    .option("relationship.target.node.keys", "REACTION:CODE")

    .save()
)



logging.info("ETL Complete Successfully")
spark.stop()