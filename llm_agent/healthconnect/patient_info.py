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

for question - Name all providers I have visited
MATCH (p:Patient {{id: "{patient_id}"}})-[:HAD_ENCOUNTER]->(:Encounter)<-[:WAS_PROVIDER_FOR_ENCOUNTER]-(pr:Provider)
return distinct pr.NAME

{question}"""

CYPHER_GENERATION_PROMPT = PromptTemplate(
    input_variables=["schema", "question", "patient_id"], template=CYPHER_GENERATION_TEMPLATE
)

QA_PROMPT_TEMPLATE = """Task: Generate a human-readable response to a question based on the data extracted from the graph database.
Instructions:
Use only the data about the medical history of the patient which is provided to you as context. The data is related to the patient's medical history. The data is extracted from the graph database using the Cypher query. The data is related to a single patient
Do not use any other data or information that is not provided.

Examples: Here are a few examples of generated responses particular questions:
for question - Which hospitals did I visit?
And the data from the graph database is: "hospitals": ["Hospital A", "Hospital B"]
The generated response should be: "You have visited Hospital A and Hospital B."

for question - What is my city? 
And the data from the graph database is: "city": "New York"
The generated response should be: "You live in New York."

for question - What is my healthcare coverage?
And the data from the graph database is: "Insurance A"
The generated response should be: "Your healthcare coverage is Insurance A."

for question - Get my entire medical history or give me info about patient? 
And the data from the graph database is: "medical_history": ["Diabetes", "Asthma"]
The generated response should be: "Your medical history includes Diabetes and Asthma."

for question - Delete my user account?
The generated response should be: "Operation not allowed, Please contact the admin to delete your account"

for question - Name all providers I have visited
And the data from the graph database is: "providers": ["Provider A", "Provider B"]
The generated response should be: "You have visited Provider A and Provider B."

the data from the graph database is: {context}
Generate a human-readable response to the question: {question}

"""

QA_GENERATION_PROMPT = PromptTemplate(
    input_variables=["question", "context"],
    template=QA_PROMPT_TEMPLATE 
)

chain = GraphCypherQAChain.from_llm(
    llm=ChatOpenAI(temperature=0, model="gpt-3.5-turbo",openai_api_key=os.environ.get("OPENAI_API_KEY")),
    graph=graph,
    verbose=True,
    cypher_prompt=CYPHER_GENERATION_PROMPT,
    qa_prompt=QA_GENERATION_PROMPT,
    validate_cypher=True,
    allow_dangerous_requests=True,
    # return_intermediate_steps=True,
    # use_function_response=True,
    # function_response_system=QA_GENERATION_PROMPT
)

def execute_query(query: str):
    return chain.invoke(
        {
            "query": query, 
            "patient_id" : os.environ.get("PATIENT_ID")
        }
    )

def get_patient_id(first_name: str, last_name: str):
    graph_query = f"MATCH (p:Patient {{ FIRST: '{first_name}', LAST: '{last_name}'}}) RETURN p.Id as patient_id"
    print(graph_query)
    graph_response = graph.query(graph_query)
    return graph_response[0]["patient_id"]