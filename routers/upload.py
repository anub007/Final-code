from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import List
from services.upload_handler import process_files
from services.uploader_factory import CloudService

router = APIRouter()

@router.post("/upload/")
async def upload_files(files: List[UploadFile] = File(...), cloud_service: CloudService = CloudService.azure):
    try:
        results = await process_files(files, cloud_service)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
