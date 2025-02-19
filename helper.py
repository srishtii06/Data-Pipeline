import json
import os
from logger import get_logger

logger=get_logger("Helpers")

PROGRESS_FILE = "progress.json"

def save_progress(file_path, row_count):
    """Save processing progress to a JSON file."""
    progress_data = {"file": file_path, "row_count": row_count}
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)

def load_progress():
    """Load the last saved progress."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return None

def update_status_file(status_file, status_dict):
    """Update the status file by appending the new file processing results."""
    # Check if status_file already exists
    if os.path.exists(status_file):
        with open(status_file, 'r') as f:
            # Load existing status data
            existing_data = json.load(f)
    else:
        existing_data = {}

    # Append new status data
    existing_data.update(status_dict)

    # Save the updated status back to the file
    with open(status_file, 'w') as f:
        json.dump(existing_data, f, indent=4)

    logger.info(f"Status of processed files has been updated in {status_file}.")    