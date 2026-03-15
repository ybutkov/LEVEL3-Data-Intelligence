from app.runtime import get_dbutils
from pathlib import Path
import json
from app.logger import get_logger
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
    # logger = get_logger(__name__)
    # json_text = json.dumps(data, ensure_ascii=False, indent=indent)
    try:
        # add time to file
        # result = get_dbutils().fs.put(full_path, json_text, overwrite=overwrite)
        # with open(full_path, "w", encoding="utf-8") as f:
        #     f.write(json_text)
        atomic_write_json(data, full_path, indent=indent)
        # logger.info(f"Saved {full_path}")

    except Exception as error:
        print(f"Error saving file: {error}")
        pass