import getpass
import os
from typing import Optional, Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import MessagesState
from IPython.display import Image, display
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition
from langchain_neo4j import Neo4jGraph

query = """
match (enc:Encounter{Id: 'B41D26FD-733E-7773-0115-A9E5C0451FBC'})
-[r:RECORDED_OBSERVATION|PERFORMED_PROCEDURE|HAS_ENCOUNTER_CLASS|HAS_ENCOUNTER_CODE|HAS_DIAGNOSIS_CODE|ADMINISTERED_IMMUNIZATION|PRESCRIBED_MEDICINE|RECORDED_CONDITION|IMPLEMENTED_CAREPLAN|PERFORMED_IMAGING_STUDY|USED_DEVICE|USED_SUPPLY|RECORDED_ALLERGY]->(t)
return
enc,
type(r) AS relationshipType, 
properties(r) AS relationshipProperties, 
labels(t) AS relatedNodeLabels, 
properties(t) AS relatedNodeProperties
"""
uri = os.environ.get("NEO4J_URI")
username = os.environ.get("NEO4J_USER")
password = os.environ.get("NEO4J_PASSWORD")
graph = Neo4jGraph(url=uri, username=username, password=password, enhanced_schema=True)

def get_information(entity: str) -> str:
    try:
        data = graph.query(query, params={"candidate": entity})
        return data
    except IndexError:
        return "No information was found"
    
class InformationInput(BaseModel):
    entity: str = Field(description="Encounter Id mentioned in the question")


class InformationTool(BaseTool):
    name: str = "Information"
    description: str = (
        "useful for when you need to answer questions about encounter details or summary"
    )
    args_schema: Type[BaseModel] = InformationInput

    def _run(
        self,
        entity: str,
    ) -> str:
        """Use the tool."""
        return get_information(entity)

    async def _arun(
        self,
        entity: str,
    ) -> str:
        """Use the tool asynchronously."""
        return get_information(entity)

llm = ChatOpenAI(model="gpt-4o")

tools = [InformationTool()]
llm_with_tools = llm.bind_tools(tools)

# System message
sys_msg = SystemMessage(
    content="You are a helpful assistant tasked with finding and explaining relevant information about encounter."
)


# Node
def assistant(state: MessagesState):
    return {"messages": [llm_with_tools.invoke([sys_msg] + state["messages"])]}




# Graph
builder = StateGraph(MessagesState)

# Define nodes: these do the work
builder.add_node("assistant", assistant)
builder.add_node("tools", ToolNode(tools))

# Define edges: these determine how the control flow moves
builder.add_edge(START, "assistant")
builder.add_conditional_edges(
    "assistant",
    # If the latest message (result) from assistant is a tool call -> tools_condition routes to tools
    # If the latest message (result) from assistant is a not a tool call -> tools_condition routes to END
    tools_condition,
)
builder.add_edge("tools", "assistant")
react_graph = builder.compile()


def get_encounter_summary(query: str) -> str:
    input_messages = [HumanMessage(content=query)]
    messages = react_graph.invoke({"messages": input_messages})
    result = []
    for m in messages["messages"]:
        if isinstance(m, BaseMessage):
            result.append(m.content)  # Access the content attribute directly
        else:
            print(f"Unexpected message type: {type(m)}")
    return result[-1]