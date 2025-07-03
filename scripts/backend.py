import sys
import os
import pandas as pd
import io
import aiohttp
from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from sqlalchemy import create_engine
from pydantic import BaseModel
import requests


sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from scripts.ai_agent import AIAgent
from scripts.data_cleaning import DataCleaning

app = FastAPI()

ai_agent = AIAgent()
cleaner = DataCleaning()

# CSV/EXCEL cleaning endpoint
@app.post("/clean_data/")
async def clean_data(file: UploadFile = File(...)):
    """
    Receives a CSV or Excel file, cleans it using rule-based and AI methods, 
    and returns cleaned data in JSON format.
    """
    try:
        contents = await file.read()
        filename = file.filename.lower()
        
        # STEP 0: Read file into DataFrame
        if filename.endswith('.csv'):
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        elif filename.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type. Upload a CSV or Excel file.")
        
        # STEP 1: Rule-based data cleaning
        df_cleaned = cleaner.clean_data(df)

        # STEP 2: AI-powered data cleaning
        df_ai_cleaned = ai_agent.process_data(df_cleaned)

        # Ensure the result is a DataFrame
        if isinstance(df_ai_cleaned, str):
            df_ai_cleaned = pd.read_csv(io.StringIO(df_ai_cleaned))
        elif not isinstance(df_ai_cleaned, pd.DataFrame):
            raise HTTPException(status_code=500, detail="AI cleaning returned invalid format.")


        return {"cleaned_data": df_ai_cleaned.to_dict(orient='records')}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
    
# Database Query Ckeaning endpoint

class DBQuery(BaseModel):
    query: str
    db_url: str

@app.post("/clean_db/")
async def clean_db(query: DBQuery):
    """
    Receives a database query and connection URL, executes the query, 
    cleans the result using AI methods, and returns cleaned data in JSON format.
    """
    try:
        engine = create_engine(query.db_url)
        df = pd.read_sql(query.query, engine)

        # STEP 1: Rule-based data cleaning
        df_cleaned = cleaner.clean_data(df)

        # STEP 2: AI-powered data cleaning
        df_ai_cleaned = ai_agent.process_data(df_cleaned)

        # Convert AI cleaned data to DataFrame 
        if isinstance(df_ai_cleaned, str):
            from io import StringIO
            df_ai_cleaned = pd.read_csv(io.StringIO(df_ai_cleaned))

        return {"cleaned_data": df_ai_cleaned.to_dict(orient='records')}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing database query: {str(e)}")
    
# API data cleaning endpoint

class APIRequest(BaseModel):
    api_url: str

@app.post("/clean_api/")
async def clean_api(api_request: APIRequest):
    """
    Receives an API URL and parameters, fetches data from the API, 
    cleans it using AI methods, and returns cleaned data in JSON format.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_request.api_url) as response:
                if response.status != 200:
                    raise HTTPException(status_code=response.status, detail="API request failed")
        data = await response.json()
        df = pd.DataFrame(data)

        # STEP 1: Rule-based data cleaning
        df_cleaned = cleaner.clean_data(df)

        # STEP 2: AI-powered data cleaning
        df_ai_cleaned = ai_agent.process_data(df_cleaned)

        # Convert AI cleaned data to DataFrame 
        if isinstance(df_ai_cleaned, str):
            from io import StringIO
            df_ai_cleaned = pd.read_csv(io.StringIO(df_ai_cleaned))

        return {"cleaned_data": df_ai_cleaned.to_dict(orient='records')}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching or processing API data: {str(e)}")
    

# Run the Server 
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app", host="127.0.0.1", port=8000, reload=True)