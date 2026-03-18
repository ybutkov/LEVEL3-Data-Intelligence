from config.config_properties import get_ConfigProperties
from config.endpoints import get_endpoint_config
from app.runtime import get_dbutils
from app.runtime import get_spark
from app.logger import get_logger
from util.path_utils import path_exists

from pyspark.sql import functions as F
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
        logger = get_logger(__name__)
        logger.error(f"Error saving file {full_path}: {error}")
        raise error
        
# ****************************************************************************

def get_bronze_source_path(endpoint, time_period: str):
    configProperties = get_ConfigProperties()
    endpoint_config = get_endpoint_config(endpoint)

    base_path = (
        f"/Volumes/{configProperties.storage.catalog}/"
        f"{configProperties.storage.bronze_schema}/"
        f"{configProperties.storage.landing_volume}/"
        f"{endpoint_config.raw_folder}"
    )
    # if time_period:
    #     return f"{base_path}/{time_period}"
    return base_path


def get_autoloader_schema_location(endpoint, time_period: str):
    configProperties = get_ConfigProperties()
    endpoint_config = get_endpoint_config(endpoint)

    base_path = (
        f"/Volumes/{configProperties.storage.catalog}/"
        f"{configProperties.storage.bronze_schema}/"
        f"{configProperties.storage.autoloader_volume}/"
        f"{endpoint_config.raw_folder}/schema"
    )
    # if time_period:
    #     return f"{base_path}/{time_period}"
    return base_path


def get_autoloader_checkpoint_location(endpoint, time_period: str):
    configProperties = get_ConfigProperties()
    endpoint_config = get_endpoint_config(endpoint)

    base_path = (
        f"/Volumes/{configProperties.storage.catalog}/"
        f"{configProperties.storage.bronze_schema}/"
        f"{configProperties.storage.autoloader_volume}/"
        f"{endpoint_config.raw_folder}/checkpoint"
    )
    # if time_period:
    #     return f"{base_path}/{time_period}"
    return base_path


def get_bronze_table_name(endpoint) -> str:
    configProperties = get_ConfigProperties()
    endpoint_config = get_endpoint_config(endpoint)

    return f"{configProperties.storage.catalog}.{configProperties.storage.bronze_schema}.{endpoint_config.bronze_table}"

# TODO: time_period ?
def load_json_to_bronze_autoloader(endpoint, time_period: str | None = None):
    logger = get_logger(__name__)
    spark = get_spark()

    source_path = get_bronze_source_path(endpoint, time_period)
    schema_location = get_autoloader_schema_location(endpoint, time_period)
    checkpoint_location = get_autoloader_checkpoint_location(endpoint, time_period)
    bronze_table = get_bronze_table_name(endpoint)

    logger.info(f"Starting Auto Loader endpoint: {endpoint.value} source={source_path} bronze_table={bronze_table}")
    if not path_exists(source_path):
        logger.warning(f"Source path: {source_path} does not exist. Nothing to load")
        logger.info(f"Finished Auto Loader endpoint:{endpoint.value} source={source_path} bronze_table={bronze_table}")
        return
    get_dbutils().fs.mkdirs(schema_location)
    get_dbutils().fs.mkdirs(checkpoint_location)
    get_dbutils().fs.mkdirs(source_path)
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", schema_location)
        # .option("multiLine", "true")
        .load(source_path)
        # .withColumnRenamed("binaryFile", "raw_json")
        .withColumn("raw_json", F.col("content").cast("string"))
        .withColumn("source_file", F.col("_metadata.file_path"))
        # .withColumn("source_file_name", F.col("_metadata.file_name"))
        # .withColumn("source_file_size", F.col("_metadata.file_size"))
        .withColumn("source_file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("bronze_ingested_at", F.current_timestamp())
        # .drop("binaryFile")
        .select(
            "raw_json",
            "source_file",
            # "source_file_name",
            # "source_file_size",
            "source_file_modification_time",
            "bronze_ingested_at",
        )
    )
    # logger.debug("1")
    query = None
    
    # delete from
    # active = spark.streams.active
    # logger.info(f"active streams count={len(active)}")
    # for q in active:
    #     logger.info(f"active stream id={q.id} name={q.name} status={q.status}")
    # logger.debug(f"table exists = {spark.catalog.tableExists(bronze_table)}")
    # to 

    try:
        query = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)
            .toTable(bronze_table)
        )
        # logger.debug("2")
        query.awaitTermination()
    finally:
        if query is not None and query.isActive:
            # logger.debug("3")
            query.stop()

    # query = (
    #     df.writeStream
    #     .format("delta")
    #     .option("checkpointLocation", checkpoint_location)
    #     .trigger(availableNow=True)
    #     .toTable(bronze_table)
    # )
    # logger.debug("4")
    # try:
    #     query.awaitTermination()
    # finally:
    #     if query.isActive:
    #         query.stop()

    # spark.stop()
    logger.info(f"Finished Auto Loader endpoint:{endpoint.value} source={source_path} bronze_table={bronze_table}")

# **************************************************************

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
   