from src.app.logger import get_logger

from pathlib import Path
import json
import os
import uuid


def atomic_write_json(data, full_path, indent=2):
    """
    Atomically write JSON data to a file using temporary file and rename.
    
    Ensures data integrity by writing to a temporary file first, then renaming.
    Creates parent directories if they don't exist. Logs success and cleans up
    temporary files on error.
    
    Args:
        data (dict): Data to serialize as JSON
        full_path (str): Target file path
        indent (int): JSON indentation level (default: 2)
        
    Raises:
        Any exceptions from file operations are logged as errors
    """
    logger = get_logger(__name__)
    path = Path(full_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + f".tmp-{uuid.uuid4().hex}")
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.replace(path)
        logger.info(f"Saved {full_path}")
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

def save_json_with_dbutils(data, full_path, overwrite = True, indent = 2):
    """
    Save JSON data to a file with error handling and logging.
    
    Wrapper around atomic_write_json that catches and logs exceptions.
    Re-raises exceptions after logging.
    
    Args:
        data (dict): Data to serialize as JSON
        full_path (str): Target file path
        overwrite (bool): Whether to overwrite existing file (default: True)
        indent (int): JSON indentation level (default: 2)
        
    Raises:
        Exception: Re-raises any exception from atomic_write_json after logging
    """
    try:
        atomic_write_json(data, full_path, indent=indent)

    except Exception as error:
        logger = get_logger(__name__)
        logger.error(f"Error saving file {full_path}: {error}")
        raise error
