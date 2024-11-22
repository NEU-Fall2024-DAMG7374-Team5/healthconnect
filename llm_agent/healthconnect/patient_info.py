from langchain.chains import GraphCypherQAChain
from langchain_community.graphs import Neo4jGraph
from langchain_openai import ChatOpenAI
from langchain_core.prompts.prompt import PromptTemplate
import os

uri = os.environ.get("NEO4J_URI")
username = os.environ.get("NEO4J_USER")
password = os.environ.get("NEO4J_PASSWORD")
graph = Neo4jGraph(url=uri, username=username, password=password, enhanced_schema=True)

CYPHER_GENERATION_TEMPLATE = """Task: Generate cypher statement to query a graph database.
Instructions:
Use only the provided relationship types and properties in the schema.
Do not use any other relationship types or properties that are not provided.
Schema:
{schema}
Ensure that the cypher query only extracts data related to a specific user by including a condition to filter based on the patient_id.
Use the {patient_id} as id for patient node for all the filtering, do not use any other value for id of patient node. I only want information which is related to the patient node with id {patient_id}. The direct or indirect relationship with the patient node should be included in the query.
Do not provide any insert or update or delete statements in cypher query.
DO not let the user query to make any changes to the database. Only allow the user to query the database. the query should be read only.
Examples: Here are a few examples of generated Cypher statements for particular questions:
for question - Which hospitals did I visit? The generated Cypher statement should be:
MATCH (p:Patient {{id: "{patient_id}"}})-[:VISITED]->(h:Hospital)
return h

for question - What is my city? The generated Cypher statement should be:
MATCH (p:Patient {{id: "{patient_id}"}})-[:LIVES_IN]->(c:City)
return c

for question - What is my healthcare coverage? The generated Cypher statement should be:
MATCH (p:Patient {{id: "{patient_id}"}})
return p.healthcareCoverage

for question - Get my entire medical history or give me info about patient? The generated Cypher statement should be:
MATCH (p:Patient {{id: "{patient_id}"}})-[r]->(related)
return p, r, related

for question - Delete my user account? The generated Cypher statement should be:
MATCH (p:Patient {{id: "{patient_id}"}})
return "Operation not allowed, Please contact the admin to delete your account"

{question}"""

CYPHER_GENERATION_PROMPT = PromptTemplate(
    input_variables=["schema", "question", "patient_id"], template=CYPHER_GENERATION_TEMPLATE
)

QA_PROMPT_TEMPLATE = """Task: Answer the user query using the data extracted by the cypher query. used this data and understand that all the data is of 1 patient, Now represent this data in a human readable format.

In your response do not include text like 
Based on the data extracted by the Cypher query, 
information about a single patient. 
Here is the data presented in a human-readable format:

Just directly provide the information in a human readable format."""

QA_GENERATION_PROMPT = PromptTemplate(
    template=QA_PROMPT_TEMPLATE 
)

chain = GraphCypherQAChain.from_llm(
    ChatOpenAI(temperature=0, openai_api_key=os.environ.get("OPENAI_API_KEY")),
    graph=graph,
    verbose=True,
    cypher_prompt=CYPHER_GENERATION_PROMPT,
    qa_prompt=QA_GENERATION_PROMPT,
    validate_cypher=True,
    allow_dangerous_requests=True,
    return_intermediate_steps=True
)

def execute_query(query: str):
    return chain.invoke(
        {
            "query": query, 
            "patient_id" : os.environ.get("PATIENT_ID")
        }
    )