import sys
import os
import pandas as pd
import io
import aiohttp
from fastapi import FastAPI, HTTPException, UploadFile, File
from sqlalchemy import create_engine
from pydantic import BaseModel
import traceback

# Append script paths
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from scripts.ai_agent import AIAgent
from scripts.data_cleaning import DataCleaning

app = FastAPI()

# Initialize AI agent with error handling
try:
    ai_agent = AIAgent()
except Exception as e:
    print(f"Warning: Could not initialize AI agent: {e}")
    ai_agent = None

# =======================
# CSV/EXCEL Cleaning Endpoint
# =======================
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

        print(f"Original data shape: {df.shape}")
        print(f"Original columns: {list(df.columns)}")

        # STEP 1: Rule-based data cleaning
        cleaner = DataCleaning(df)
        cleaner.clean_data()
        df_cleaned = cleaner.get_clean_data()
        
        print(f"After rule-based cleaning shape: {df_cleaned.shape}")
        print(f"After rule-based cleaning columns: {list(df_cleaned.columns)}")

        # STEP 2: AI-powered data cleaning (optional, only if AI agent is available)
        if ai_agent:
            try:
                ai_results = ai_agent.process_data(df_cleaned)
                print(f"AI analysis completed with {len(ai_results)} batch results")
                
                # Return both cleaned data and AI analysis
                return {
                    "cleaned_data": df_cleaned.to_dict(orient='records'),
                    "ai_analysis": ai_results,
                    "original_shape": df.shape,
                    "cleaned_shape": df_cleaned.shape
                }
            except Exception as ai_error:
                print(f"AI processing failed: {ai_error}")
                # Return cleaned data without AI analysis
                return {
                    "cleaned_data": df_cleaned.to_dict(orient='records'),
                    "ai_analysis": [{"error": f"AI processing failed: {str(ai_error)}"}],
                    "original_shape": df.shape,
                    "cleaned_shape": df_cleaned.shape
                }
        else:
            # Return cleaned data without AI analysis
            return {
                "cleaned_data": df_cleaned.to_dict(orient='records'),
                "ai_analysis": [{"error": "AI agent not available"}],
                "original_shape": df.shape,
                "cleaned_shape": df_cleaned.shape
            }

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


# =======================
# Database Query Cleaning Endpoint
# =======================
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
        cleaner = DataCleaning(df)
        cleaner.clean_data()
        df_cleaned = cleaner.get_clean_data()

        # STEP 2: AI-powered data cleaning (optional)
        if ai_agent:
            try:
                ai_results = ai_agent.process_data(df_cleaned)
                return {
                    "cleaned_data": df_cleaned.to_dict(orient='records'),
                    "ai_analysis": ai_results
                }
            except Exception as ai_error:
                return {
                    "cleaned_data": df_cleaned.to_dict(orient='records'),
                    "ai_analysis": [{"error": f"AI processing failed: {str(ai_error)}"}]
                }
        else:
            return {
                "cleaned_data": df_cleaned.to_dict(orient='records'),
                "ai_analysis": [{"error": "AI agent not available"}]
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing database query: {str(e)}")


# =======================
# API Data Cleaning Endpoint
# =======================
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
        cleaner = DataCleaning(df)
        cleaner.clean_data()
        df_cleaned = cleaner.get_clean_data()

        # STEP 2: AI-powered data cleaning (optional)
        if ai_agent:
            try:
                ai_results = ai_agent.process_data(df_cleaned)
                return {
                    "cleaned_data": df_cleaned.to_dict(orient='records'),
                    "ai_analysis": ai_results
                }
            except Exception as ai_error:
                return {
                    "cleaned_data": df_cleaned.to_dict(orient='records'),
                    "ai_analysis": [{"error": f"AI processing failed: {str(ai_error)}"}]
                }
        else:
            return {
                "cleaned_data": df_cleaned.to_dict(orient='records'),
                "ai_analysis": [{"error": "AI agent not available"}]
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching or processing API data: {str(e)}")


# =======================
# Health Check Endpoint
# =======================
@app.get("/health")
async def health_check():
    """Health check endpoint to verify the service is running."""
    return {
        "status": "healthy",
        "ai_agent_available": ai_agent is not None
    }


# =======================
# Run the Server
# =======================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend:app", host="127.0.0.1", port=8000, reload=True)