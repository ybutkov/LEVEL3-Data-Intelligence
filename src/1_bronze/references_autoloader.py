import sys

root_path = spark.conf.get("root_path")
if root_path and root_path not in sys.path:
    sys.path.insert(0, root_path)

from src.config.endpoints import EndpointKeys
from src.config.endpoints import get_endpoint_config
from src.config.endpoints import EndpointConfig
from src.config.config_properties import ConfigProperties
from src.config.config_properties import get_ConfigProperties
from src.util.tables_utils import build_full_table_name

from pyspark import pipelines as dp
from pyspark.sql import functions as F

import uuid

CATALOG = spark.conf.get("catalog")
SCHEMA = spark.conf.get("bronze_schema")
LANDING_VOLUME = spark.conf.get("landing_area")
META_VOLUME = spark.conf.get("meta_volume")


ENDPOINTKEYS_PIPELINE_LIST = [
    EndpointKeys.AIRPORTS,
    EndpointKeys.CITIES,
    EndpointKeys.COUNTRIES,
    EndpointKeys.AIRLINES,
    EndpointKeys.AIRCRAFT,
]

INGEST_RUN_ID = str(uuid.uuid4())


def build_source_path(endpoint_config: EndpointConfig) -> str:
    return (
        f"/Volumes/{CATALOG}/"
        f"{SCHEMA}/"
        f"{LANDING_VOLUME}/"
        f"{endpoint_config.raw_folder}"
    )


def build_schema_location(endpoint_config: EndpointConfig) -> str:
    return (
        f"/Volumes/{CATALOG}/"
        f"{SCHEMA}/"
        f"{META_VOLUME}/"
        f"{endpoint_config.raw_folder}/"
        f"schema"
    )


def build_stream(endpoint_config: EndpointConfig, config_properties: ConfigProperties):
    source_path = build_source_path(endpoint_config)
    schema_location = build_schema_location(endpoint_config)

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", schema_location)
        .load(source_path)
    )

    df = (
        df
        .withColumn("raw_json", F.col("content").cast("string"))
        .withColumnRenamed("path", "source_file")
        .withColumn("bronze_ingested_at", F.current_timestamp())
        .withColumn("ingest_run_id", F.lit(INGEST_RUN_ID).cast("string"))
        .drop("content", "length")
    )

    fixed_first_cols = [
        "bronze_ingested_at",
        "source_file",
        "modificationTime",
    ]

    fixed_last_cols = ["raw_json"]

    middle_cols = [
        c for c in df.columns
        if c not in set(fixed_first_cols + fixed_last_cols)
    ]

    df = df.select(
        *fixed_first_cols,
        *middle_cols,
        *fixed_last_cols,
    )

    return df


def register_raw_table(endpoint_config: EndpointConfig, config_properties: ConfigProperties):
    # @dp.table(name=endpoint_config.bronze_table)
    @dp.table(name=build_full_table_name(CATALOG, SCHEMA, endpoint_config.bronze_table))
    def _table():
        return build_stream(endpoint_config, config_properties)

    return _table


for endpoint_key in ENDPOINTKEYS_PIPELINE_LIST:
    endpoint_config = get_endpoint_config(endpoint_key)
    config_properties = get_ConfigProperties()
    register_raw_table(endpoint_config=endpoint_config, config_properties=config_properties)
