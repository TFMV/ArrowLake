## Thomas F McGeehan V
## Free to use under MIT License

import os
import logging
import asyncio
import time
from urllib.parse import quote
import datafusion
from pyarrow.fs import GcsFileSystem
import asyncpg
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException

app = FastAPI()
logging.basicConfig(level=logging.INFO)

class IngestionRequest(BaseModel):
    bucket_name: str
    file_name: str
    instance_connection_name: str
    dbname: str
    table_name: str
    gcp_project: str

def get_secret(secret_name: str, project_id: str):
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    print(f"Project ID: {project_id}")
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    print(f"Secret: {response.payload.data.decode('UTF-8')}")
    return response.payload.data.decode("UTF-8")

async def connect_db(ingestion_request: IngestionRequest):
    password = get_secret(
        f"{ingestion_request.dbname}-pwd",
        ingestion_request.gcp_project,
    )
    encoded_password = quote(password)
    user = os.getenv('DB_USER', "postgres")
    db_name = ingestion_request.dbname

    socket_dir = '/cloudsql'
    socket_path = f"{socket_dir}/{ingestion_request.instance_connection_name}"
    uri = f"postgresql://{user}:{encoded_password}@/{db_name}?host={socket_path}"

    conn = await asyncpg.connect(uri)
    return conn

async def ingest_data(ingestion_request: IngestionRequest, conn):
    try:
        start_time = time.time()
        ctx = datafusion.SessionContext()
        with GcsFileSystem().open_input_stream(f"{ingestion_request.bucket_name}/{ingestion_request.file_name}") as f:
            result = await conn.copy_to_table(
                table_name=ingestion_request.table_name,
                source=f,
            )
            elapsed_time = time.time() - start_time
            return result, elapsed_time
    except Exception as e:
        logging.error(f"Error during data ingestion: {e}")
        raise

@app.post("/ingest")
async def ingest_endpoint(ingestion_request: IngestionRequest):
    try:
        conn = await connect_db(ingestion_request)
        result, elapsed_time = await ingest_data(ingestion_request, conn)
        return {
            "message": f"Rows loaded: {result}",
            "Elapsed Seconds": elapsed_time
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    return {"message": "Ingestion API Ready"}

async def main():
    ig = IngestionRequest(
        bucket_name=os.getenv("GCS_BUCKET_NAME", "tfmv"),
        file_name="ints.parquet",
        instance_connection_name=os.getenv("CLOUD_SQL_INSTANCE", "tfmv-371720:us-central1:tfmv15"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        table_name="tfmv",
        gcp_project=os.getenv("GCP_PROJECT", "tfmv-371720")
    )
    conn = None
    try:
        conn = await connect_db(ig)
        result, elapsed_time = await ingest_data(ig, conn)
        logging.info(f"Ingestion result: {result}, Time taken: {elapsed_time} seconds")
    except Exception as e:
        logging.error(f"Unexpected error in main: {e}")
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    asyncio.run(main())