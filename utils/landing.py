import requests
import json
import datetime
import config.config as cfg
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def save_json_with_dbutils(data, full_path, overwrite = True, indent = 2):
    json_text = json.dumps(data, ensure_ascii=False, indent=indent)
    try:
        # add time to file
        dbutils.fs.put(full_path, json_text, overwrite=overwrite)
    except Exception:
        print("Error saving file")
        pass


def get_full_save_path(endpoint, offset=None, time_dir=None):
    name = endpoint.value
    filename = name
    if offset or offset == 0:
        filename = f"{filename}_{offset}"
    filename = f"{filename}.json"
    if time_dir:
        # month = datetime.datetime.now().strftime(ts_format)
        target_dir = f"{cfg.PROFILE.get(cfg.ProfileKeys.LANDING_ROOT)}/{name}/{time_dir}"
        # print(target_dir)
    else:
        target_dir = f"{cfg.PROFILE.get(cfg.ProfileKeys.LANDING_ROOT)}/{name}"
    full_save_path = f"{target_dir}/{filename}"
    # print(f"get_full_save_path combine path={full_save_path} offset={offset}")
    return full_save_path
