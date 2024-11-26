from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import agent
import patient_info
app = FastAPI()

class QueryRequest(BaseModel):
    query: str
    patientid: str

class PatientRequest(BaseModel):
    first_name: str
    last_name: str

@app.post("/process_query")
async def process_query(request: QueryRequest):
    try:
        response = agent.process_query(request.query, request.patientid)
        return {"status": "success", "data": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/get_patient_id")
async def get_patient_id(request: PatientRequest):
    try:
        patient_id = patient_info.get_patient_id(request.first_name, request.last_name)
        return {"status": "success", "patient_id": patient_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))