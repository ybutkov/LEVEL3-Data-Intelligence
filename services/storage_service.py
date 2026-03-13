from runtime import get_dbutils
import json

def save_json_with_dbutils(data, full_path, overwrite = True, indent = 2):
    json_text = json.dumps(data, ensure_ascii=False, indent=indent)
    try:
        # add time to file
        get_dbutils().fs.put(full_path, json_text, overwrite=overwrite)
    except Exception:
        print("Error saving file")
        pass