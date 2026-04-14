from fastapi import FastAPI
from pipeline.executor import PipelineExecutor
from database.fetch import FetchData
from database.connect_db import get_connection

app = FastAPI(
    title="Self-Healing Data Pipeline API",
    description="API for running pipeline and fetching data",
    version="1.0"
)



# ROOT API

@app.get("/")
def home():
    return {
        "message": " Self-Healing Pipeline API is running"
    }



# RUN PIPELINE

@app.get("/run-pipeline")
def run_pipeline():
    try:
        PipelineExecutor.run()
        return {"status": "Pipeline executed successfully"}
    except Exception as e:
        return {"error": str(e)}



# FETCH ERROR DATA

@app.get("/error-data")
def get_error_data():
    try:
        data = FetchData.fetch_error_records()
        return {
            "count": len(data),
            "data": data
        }
    except Exception as e:
        return {"error": str(e)}



# FETCH CLEANED DATA 

@app.get("/cleaned-data")
def get_cleaned_data():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM customer_cleaned")
        data = cursor.fetchall()

        return {
            "count": len(data),
            "data": data
        }

    except Exception as e:
        return {"error": str(e)}



# PIPELINE RUN HISTORY

@app.get("/pipeline-runs")
def get_pipeline_runs():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 10")
        data = cursor.fetchall()

        return {
            "count": len(data),
            "data": data
        }

    except Exception as e:
        return {"error": str(e)}