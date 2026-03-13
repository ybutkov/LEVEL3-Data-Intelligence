import requests
import json
import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, ArrayType
import config.config as cfg
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def save_json_with_dbutils(data, full_path, overwrite = True, indent = 2):
    json_text = json.dumps(data, ensure_ascii=False, indent=indent)
    try:
        # add time to file
        print(dbutils)
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
  
def load_data_to_bronze(endpoint_key):
    entity = endpoint_key.value
    target_dir = f"{cfg.PROFILE.get(cfg.ProfileKeys.LANDING_ROOT)}/{entity}"
    target_table = cfg.PROFILE.get(cfg.ProfileKeys.BRONZE_ROOT).format(entity_name= entity)
    print(target_dir)
    print(target_table)
    # full_save_path = f"{target_dir}/*.json"
    raw_df = (spark.read
          .option("multiline", "true")
          .option("recursiveFileLookup", "true")
          .json(target_dir)
          .select("*", "_metadata.file_path")
          .withColumnRenamed("file_path", "_source_file")
          .withColumn("_ingestion_timestamp", F.current_timestamp())
    )
        
    # display(raw_df)
    print(f"Loaded {raw_df.count()} records from {target_dir}")
    raw_df.printSchema()

    # technical_cols = {"_source_file", "_ingestion_timestamp"}

    # root_candidates = [
    #     field for field in raw_df.schema.fields
    #     if field.name not in technical_cols and isinstance(field.dataType, StructType)
    # ]

    # if not root_candidates:
    #     raise ValueError("No root struct column found in JSON.")

    # root_col = root_candidates[0].name
    # root_type = root_candidates[0].dataType

    # child_struct_candidates = [
    #     field for field in root_type.fields
    #     if isinstance(field.dataType, StructType)
    # ]

    # if not child_struct_candidates:
    #     raise ValueError(f"No child struct found inside root column '{root_col}'.")

    # child_struct_col = child_struct_candidates[0].name
    # child_struct_type = child_struct_candidates[0].dataType

    # array_candidates = [
    #     field for field in child_struct_type.fields
    #     if isinstance(field.dataType, ArrayType)
    # ]

    # if not array_candidates:
    #     raise ValueError(
    #         f"No array field found inside '{root_col}.{child_struct_col}'."
    #     )

    # array_col = array_candidates[0].name

    # full_array_path = f"{root_col}.{child_struct_col}.{array_col}"

    # print(f"Detected root_col       : {root_col}")
    # print(f"Detected child_struct   : {child_struct_col}")
    # print(f"Detected array_col      : {array_col}")
    # print(f"Full array path         : {full_array_path}")

    # exploded_df = raw_df.select(
    #     F.explode_outer(F.col(full_array_path)).alias("item"),
    #     "_source_file",
    #     "_ingestion_timestamp"
    # )

    # bronze_df = exploded_df.select(
    #     "item.*",
    #     "_source_file",
    #     "_ingestion_timestamp"
    # )
    #
    bronze_df = raw_df
    #

    print(f"Rows to write: {bronze_df.count()}")
    bronze_df.printSchema()

    bronze_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)

    print(f"Loaded data into {target_table}")
