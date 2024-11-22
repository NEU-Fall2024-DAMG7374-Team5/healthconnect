from langchain_core.runnables.graph import CurveStyle, MermaidDrawMethod, NodeStyles
from agent import get_runnable

runnable = get_runnable()
image_data = runnable.get_graph().draw_mermaid_png(draw_method=MermaidDrawMethod.API)

with open("graph.png", "wb") as file:
    file.write(image_data)