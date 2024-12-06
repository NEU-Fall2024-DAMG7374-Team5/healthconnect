from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import agent, json, ast
import patient_info, encounter_summary
app = FastAPI()

class QueryRequest(BaseModel):
    query: str
    patientid: str

class PatientRequest(BaseModel):
    first_name: str
    last_name: str

@app.post("/process_query")
async def process_query(request: QueryRequest):
    response = agent.process_query(request.query, request.patientid)
    try:
        response_dict = ast.literal_eval(response)
        if "result" in response_dict:
            question = patient_info.generate_follow_up_question(request.query, response_dict["result"])
            return {"status": "success", "data": response_dict['result'], "follow_up_question": question}
        return {"status": "success", "data": response}
    except:
        question = patient_info.generate_follow_up_question(request.query,"")
        return {"status": "success", "data": response, "follow_up_question": question}

@app.post("/get_patient_id")
async def get_patient_id(request: PatientRequest):
    try:
        patient_id = patient_info.get_patient_id(request.first_name, request.last_name)
        return {"status": "success", "patient_id": patient_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/encounter_summary")
async def get_encounter_summary(request: QueryRequest):
    try:
        response = encounter_summary.get_encounter_summary(request.query)
        return {"status": "success", "data": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))