from pyspark import pipelines as dp
from pyspark.sql import functions as F

from config.reference_pipeline import REFERENCE_PIPELINE_CONFIG

CATALOG = "lufthansa_level"
SCHEMA = "bronze"
LANDING_VOLUME = "landing_area"
META_VOLUME = "autoloader_metadata"


def build_source_path(entity: str, time_period: str) -> str:
    base_path = (
        f"/Volumes/{CATALOG}/"
        f"{SCHEMA}/"
        f"{META_VOLUME}/"
        f"{entity}"
    )
    return base_path


def build_schema_location(entity: str, time_period: str) -> str:
    base_path = (
        f"/Volumes/{CATALOG}/"
        f"{SCHEMA}/"
        f"{META_VOLUME}/"
        f"{entity}/schema"
    )
    return base_path

# TODO: time_period ?
def build_stream(entity: str, time_period: str):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", build_schema_location(entity, load_type))
        .load(build_source_path(entity, load_type))
        .select(
            F.col("content").cast("string").alias("raw_json"),
            F.col("path").alias("source_file"),       
            F.col("modificationTime").alias("source_file_modification_time"),          
            F.current_timestamp().alias("bronze_ingested_at"),
            F.col("length").alias("source_file_size"),
            F.lit(entity).alias("entity_name"),
            # F.lit(load_type).alias("time_period"),
        )
    )


def register_raw_table(entity: str, table_name: str, time_period: str):
    @dp.table(name=table_name)
    def _table():
        return build_stream(entity, time_period)

    return _table


for entity_name, entity_cfg in REFERENCE_PIPELINE_CONFIG.items():
    register_raw_table(
        entity=entity_name,
        table_name=entity_cfg["table_name"],
        time_period=entity_cfg["time_period"],
    )
