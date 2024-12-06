from typing import TypedDict, Annotated
from langchain_core.agents import AgentAction
from langchain_core.messages import BaseMessage
import operator
from langchain_core.tools import tool
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_openai import ChatOpenAI
from typing import TypedDict
from langgraph.graph import StateGraph, END
from dotenv import load_dotenv
import os
from palmyra import get_palmyra_response
from patient_info import execute_query
from fastapi import HTTPException
import encounter_summary

load_dotenv()

class AgentState(TypedDict):
   input: str
   chat_history: list[BaseMessage]
   intermediate_steps: Annotated[list[tuple[AgentAction, str]], operator.add]

@tool("palmyra_response")
def palmyra_response(query: str):
   """Returns a natural language response to the user query using the palmyra model if it is a generic medical query."""
   return get_palmyra_response(query)

@tool("get_patient_info")
def get_patient_info(query: str):
   """Fetches the data related to the patient medical history using the query from the graph database if it is required."""
   return execute_query(query)


system_prompt = """You are the oracle, the great AI decision maker.
Given the user's query you must identify if it is a generic medical query or a query related to patients medical history.
If it is a generic medical query, use the palmyra_response to generate a response by passing the exact user provided query to the tool.
If it is a query related to the patient's medical history, use the get_patient_info tool by passing the exact user provided query to the tool which uses graph database to fetch the required information.
If it is a query related to the encounter details with an encounter id, use the get_encounter_info tool by passing the exact user provided query to the tool.
You can use the following tools:
- palmyra_response
- get_patient_info
- get_encounter_info

If you see that a tool has been used (in the scratchpad) with a particular
query, do NOT use that same tool with the same query again. Also, do NOT use
any tool more than 2 times (ie, if the tool appears in the scratchpad 2 times, do
not use it again).

Once you have collected information
to answer the user's question (stored in the scratchpad) use the palmyra model to generate a response."""

prompt = ChatPromptTemplate.from_messages([
   ("system", system_prompt),
   MessagesPlaceholder(variable_name="chat_history"),
   ("user", "{input}"),
   ("assistant", "scratchpad: {scratchpad}"),
])


llm = ChatOpenAI(
   model="gpt-3.5-turbo",  # or "gpt-4" if you have access
   openai_api_key=os.environ.get("OPENAI_API_KEY"), # your openai api key
   temperature=0
)

tools=[
   palmyra_response,
   get_patient_info
]

def create_scratchpad(intermediate_steps: list[AgentAction]):
   research_steps = []
   for i, action in enumerate(intermediate_steps):
       if action.log != "TBD":
           research_steps.append(
               f"Tool: {action.tool}, input: {action.tool_input}\n"
               f"Output: {action.log}"
           )
   return "\n---\n".join(research_steps)


oracle = (
   {
       "input": lambda x: x["input"],
       "chat_history": lambda x: x["chat_history"],
       "scratchpad": lambda x: create_scratchpad(
           intermediate_steps=x["intermediate_steps"]
       ),
   }
   | prompt
   | llm.bind_tools(tools, tool_choice="auto")
)

def run_oracle(state: TypedDict):
   out = oracle.invoke(state)
   tool_name = out.tool_calls[0]["name"]
   tool_args = out.tool_calls[0]["args"]
   action_out = AgentAction(
       tool=tool_name,
       tool_input=tool_args,
       log="TBD"
   )
   return {
       "intermediate_steps": [action_out]
   }


def router(state: TypedDict):
   if isinstance(state["intermediate_steps"], list):
      return state["intermediate_steps"][-1].tool
   else:
      return "palmyra_response"
   

tool_str_to_func = {
   "palmyra_response": palmyra_response,
   "get_patient_info": get_patient_info
}


def run_tool(state: TypedDict):
   tool_name = state["intermediate_steps"][-1].tool
   tool_args = state["intermediate_steps"][-1].tool_input
   out = tool_str_to_func[tool_name].invoke(input=tool_args)
   action_out = AgentAction(
       tool=tool_name,
       tool_input=tool_args,
       log=str(out)
   )
   return {"intermediate_steps": [action_out]}


graph = StateGraph(AgentState)
graph.add_node("oracle", run_oracle)
graph.add_node("palmyra_response", run_tool)
graph.add_node("get_patient_info", run_tool)
graph.set_entry_point("oracle")
graph.add_edge("palmyra_response", END)
graph.add_edge("get_patient_info", "palmyra_response")
graph.add_edge("get_patient_info", END)
graph.add_conditional_edges(
   source="oracle",  # where in graph to start
   path=router,  # function to determine which node is called
)
runnable = graph.compile()


def process_query(user_query: str, patient_id: str):
   os.environ["PATIENT_ID"] = patient_id
   state = {
      "input": user_query,
      "chat_history": [],
   }
   try:
      result = runnable.invoke(state)
      if not result["intermediate_steps"]:
         raise ValueError("No intermediate steps found in the result.")
       
      output = result["intermediate_steps"][-1].log
      return output
   except IndexError as e:
      raise HTTPException(status_code=500, detail="IndexError: list index out of range")
   except Exception as e:
      raise HTTPException(status_code=500, detail=str(e))
   

def get_runnable():
   return runnable