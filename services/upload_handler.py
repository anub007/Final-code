import tempfile
import time
import asyncio
from fastapi import UploadFile, HTTPException
from typing import List
from services.uploader_factory import CloudService, get_uploader
import os
import logging
import traceback

logger = logging.getLogger(__name__)


async def process_single_file(file: UploadFile, cloud_service: CloudService):
    uploader = get_uploader(cloud_service)
    try:
        start_time = time.time()
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            chunk_size = 1024 * 1024

            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                temp_file.write(chunk)

        await asyncio.to_thread(uploader.upload_stream, temp_file_path, file.filename)
        duration = time.time() - start_time
        logger.info(f"Upload of {file.filename} completed in {duration} seconds")
        return {"filename": file.filename, "message": f"Upload completed successfully in {duration} seconds."}

    except Exception as e:
        logger.error(f"Failed to upload file {file.filename} due to: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to upload file {file.filename}.")

    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)


async def process_files(files: List[UploadFile], cloud_service: CloudService):
    # Process each file asynchronously
    upload_tasks = [process_single_file(file, cloud_service) for file in files]
    results = await asyncio.gather(*upload_tasks)
    return results
