import os
import logging
import json
import uuid
from azure.storage.blob import BlobServiceClient
from .uploader import Uploader
from threading import Lock

#logger = logging.getLogger(__name__)

class BlobUploader1(Uploader):
    lock = Lock()  # Lock to avoid simultaneous writes to state files. Prevents race condition

    def __init__(self, connection_string: str, container_name: str):
        if not connection_string or not container_name:
            logger.error("Azure storage connection string or container name is missing.")
            raise ValueError("Missing Azure connection string or container name")

        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        # self.upload_state_file = "azure_upload_state.json"

    def _get_state_file_path(self, blob_name):
        """
        Generate the file path for the state file associated with a given blob name.
        Args:
            blob_name (str): The name of the blob for which the state file path is to be generated.
        Returns:
            str: The full file path to the state file, which is located in the same directory as the current file.
        """
        # Get the current working directory of the project
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # This gets the directory of the current file (project root)

        state_dir = os.path.join(project_dir, "upload_state_logs")
        # Construct the state file path using the blob name, appending '_state.json' to the blob name
        state_file_path = os.path.join(state_dir, f"{blob_name}_state.json")

        return state_file_path
    
    def _load_state(self, blob_name): 
        """
        Loads the upload state from a JSON file for the given blob name.

        Args:
            blob_name (str): The name of the blob for which to load the state.

        Returns:
            dict or None: The upload state as a dictionary if the state file exists and is valid,
                          otherwise None. If the state file is corrupted, logs an error and returns None.
        """
        state_file = self._get_state_file_path(blob_name)
        if os.path.exists(state_file):
            with self.lock:
                try:
                    with open(state_file, "r") as f:
                        upload_state = json.load(f)
                    return upload_state
                except json.JSONDecodeError:
                    logger.error(f"State file {state_file} is corrupted. Starting fresh upload.")
                    return None
        return None
    
    
    def _save_state(self, blob_name, state):
        """
        Saves the given state to a file associated with the specified blob name.

        Args:
            blob_name (str): The name of the blob for which the state is being saved.
            state (dict): The state data to be saved.

        Returns:
            None
        """
        state_file = self._get_state_file_path(blob_name)
        with self.lock:
                try:
                    with open(state_file, "w") as f:
                        json.dump(state, f)
                except Exception as e:
                    logger.error(f"Error saving state file {state_file}: {str(e)}")


    def upload_stream(self, file_path: str, blob_name: str, chunk_size: int = 4 * 1024 * 1024, max_retries: int = 3):
        """
        Uploads a file to Azure Blob Storage in chunks, with support for resuming interrupted uploads.
        Args:
            file_path (str): The local path to the file to be uploaded.
            blob_name (str): The name of the blob in Azure Blob Storage.
            chunk_size (int, optional): The size of each chunk to upload in bytes. Default is 4 MB.
            max_retries (int, optional): The maximum number of retries for each chunk upload in case of failure. Default is 3.
        Raises:
            Exception: If an error occurs during the upload process that exceeds the maximum number of retries.
        Notes:
            - The method supports resuming interrupted uploads by saving the state after each successful chunk upload.
            - The state is saved in a file and loaded if available to resume the upload from where it left off.
            - After successfully uploading all chunks, the method commits the blocks and cleans up the state file.
        """
        # chunk size 100 MB
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            file_size = os.path.getsize(file_path)
            block_ids = []
            uploaded_size = 0

            # Load existing state if available, to resume
            state = self._load_state(blob_name)
            if state:
                uploaded_size = state["uploaded_size"]
                block_ids = state["block_ids"]

            with open(file_path, "rb") as file:
                file.seek(uploaded_size)  # Resume from where left off

                while uploaded_size < file_size:
                    chunk = file.read(chunk_size)
                    if not chunk:
                        break

                    block_id = str(uuid.uuid4())
                    retries = 0
                    while retries <= max_retries:
                        try:
                            # Stage the block (upload chunk)
                            blob_client.stage_block(block_id=block_id, data=chunk)
                            block_ids.append(block_id)
                            uploaded_size += len(chunk)
                            break  # Exit retry loop on success
                        except Exception as e:
                            retries += 1
                            logger.warning(f"Retry {retries}/{max_retries} for block upload due to: {str(e)}")
                            if retries > max_retries:
                                raise

                    # Save state after each successful block upload
                    self.progress = (uploaded_size / file_size) * 100
                    state = {
                        "blob_name": blob_name,
                        "uploaded_size": uploaded_size,
                        "block_ids": block_ids
                    }
                    self._save_state(blob_name, state)
                    logger.info(f"Uploaded {uploaded_size} of {file_size} bytes ({self.progress:.2f}%)")

            # Commit all blocks after completing all chunks
            blob_client.commit_block_list(block_ids)

            # Clean up state file on successful upload
            state_file = self._get_state_file_path(blob_name)
            if os.path.exists(state_file):
                os.remove(state_file)

        except Exception as e:
            logger.error(f"Error during Azure Blob upload: {str(e)}")
            raise
