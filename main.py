import os
import google.generativeai as genai
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel,field_validator
from typing import Union
import json
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

# loading api
load_dotenv()
try:
    genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))
except KeyError:
    print("ERROR in api key")

# Data Models (Input and Output)
class QueryInput(BaseModel):
    sql_query:str
    execution_time_sec:float
    table_schema:str
    
class Output(BaseModel):
    summary:str
    recommendation:Union[str, list[str]]
    optimized_query:str

    

# prompt Eng
def optimized_prompt(query_input: QueryInput) -> str:
    x = f"""
       Analyze the provided SQL query based on the schema and execution time. 
Respond ONLY with a valid JSON object with the keys "summary", "recommendation", and "optimized_query". 
Do not add any text, comments, or markdown formatting outside of the JSON object.

- "summary": A simple, one-sentence explanation of why the query is slow, written in plain English for a non-technical project manager. Avoid technical jargon.
- "recommendation": Suggest a query rewrite if possible; The recommendation must include the simple changes that can be made in the query to optimize the duration and make query faster and simpler.
- "optimized_query": A more efficient version of the provided SQL query in terms of duration and provide the query that would take less time. If no optimization is possible, return the same query.

### START DATA ###
execution_time_seconds: {query_input.execution_time_sec}
query: {query_input.sql_query}
schema: {query_input.table_schema}
### END DATA ###"""


    return x

# api server setup
app = FastAPI(
title="AI Database Profiler",
description="An API that uses Gemini 2.5 Pro to optimize slow SQL queries.",
version="1.0.0",
)

origins = ["http://localhost:3000"]
app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials = True,
    allow_methods = ["*"],
    allow_headers = ["*"]
)

# api endpoints
@app.get("/",tags=['Health Check'])
async def read_root():
    return {"status":"ok",
            "message":"AI Database Profiler is running"}

@app.post('/analyze-query-gemini',response_model=Output,tags=["analysis"])
async def analyze_query(query_input:QueryInput):
    prompt = optimized_prompt(query_input)
    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
        response = model.generate_content(prompt, generation_config=generation_config)

        text = response.text
        print(f" AI Response Received:\n{text}")

        recommendation = Output.parse_raw(text)
        return recommendation
    except json.JSONDecodeError:
        error_detail = f"AI did not return valid JSON. Response: {text}"
        print(f" ERROR: {error_detail}")
        raise HTTPException(status_code=500, detail=error_detail)
    except Exception as e:
        error_detail = f"An unexpected error occurred: {e}"
        print(f" ERROR: {error_detail}")
        raise HTTPException(status_code=500, detail=error_detail)