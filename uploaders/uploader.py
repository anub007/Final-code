import os
import json
from abc import ABC, abstractmethod

class Uploader(ABC):
    @abstractmethod
    def upload_stream(self, file_path: str, blob_name: str):
        pass

    def _get_state_file_path(self, blob_name: str) -> str:
        """
        Generate the file path for the state file associated with a given blob name.
        Args:
            blob_name (str): The name of the blob for which the state file path is to be generated.
        Returns:
            str: The full file path to the state file, which is located in a specified subdirectory under the parent directory of the project.
        """
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # This gets the parent directory of the current file's directory
        state_dir = os.path.join(project_dir, "state_files")
        os.makedirs(state_dir, exist_ok=True)
        state_file_path = os.path.join(state_dir, f"{blob_name}_state.json")
        return state_file_path

    def _load_state(self, blob_name: str) -> dict:
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
            try:
                with open(state_file, "r") as f:
                    upload_state = json.load(f)
                return upload_state
            except json.JSONDecodeError:
                print(f"State file {state_file} is corrupted. Starting fresh upload.")
                return None
        return None


    def _save_state(self, blob_name: str, state: dict):
        """
        Saves the given state to a file associated with the specified blob name.
        Args:
            blob_name (str): The name of the blob for which the state is being saved.
            state (dict): The state data to be saved.
        Returns:
            None
        """
        state_file = self._get_state_file_path(blob_name)
        try:
            with open(state_file, "w") as f:
                json.dump(state, f)
        except Exception as e:
            print(f"Error saving state file {state_file}: {str(e)}")