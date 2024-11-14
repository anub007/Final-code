import boto3
import logging
from .uploader import Uploader

logger = logging.getLogger(__name__)

class S3Uploader(Uploader):
    def __init__(self, access_key: str, secret_key: str, bucket_name: str, region_name: str):
        pass

    def upload_stream(self, file_path: str, object_name: str):
        pass
