from enum import Enum
from uploaders.BlobUploader import BlobUploader
from uploaders.S3Uploader import S3Uploader
import os

# Define cloud service options as an Enum
class CloudService(str, Enum):
    azure = "azure"
    s3 = "s3"

# Factory function to select and create the uploader
def get_uploader(cloud_service: CloudService):
    if cloud_service == CloudService.azure:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        return BlobUploader(connection_string, container_name)
    elif cloud_service == CloudService.s3:
        access_key = os.getenv("AWS_ACCESS_KEY")
        secret_key = os.getenv("AWS_SECRET_KEY")
        bucket_name = os.getenv("AWS_BUCKET_NAME")
        region_name = os.getenv("AWS_REGION")
        return S3Uploader(access_key, secret_key, bucket_name, region_name)
    else:
        raise ValueError("Unsupported cloud service. Choose 'azure' or 's3'.")
