from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import agent
app = FastAPI()

class QueryRequest(BaseModel):
    query: str
    patientid: str


@app.post("/process_query")
async def process_query(request: QueryRequest):
    try:
        response = agent.process_query(request.query, request.patientid)
        return {"status": "success", "data": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))