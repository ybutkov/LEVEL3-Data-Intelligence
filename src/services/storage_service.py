from src.app.logger import get_logger

from pathlib import Path
import json
import os
import uuid


def atomic_write_json(data, full_path, indent=2):
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
    try:
        atomic_write_json(data, full_path, indent=indent)

    except Exception as error:
        logger = get_logger(__name__)
        logger.error(f"Error saving file {full_path}: {error}")
        raise error
