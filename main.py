from fastapi import FastAPI
from dotenv import load_dotenv
import logging
from routers import upload

import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("app.log")]
)

app = FastAPI()

# Include the upload router
app.include_router(upload.router)


# Add a check to see which uploader is being used
@app.on_event("startup")
async def startup_event():
    print("Starting FastAPI application")
    print("Using cloud service:", os.getenv("CLOUD_SERVICE"))
    print("Azure connection string:", os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
    print("Azure container name:", os.getenv("AZURE_STORAGE_CONTAINER_NAME"))
    return "Azure is connected "
