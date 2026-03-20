from src.config.endpoints import EndpointKeys
from src.config.endpoints import get_endpoint_config
from src.config.endpoints import EndpointConfig
from src.config.config_properties import ConfigProperties
from src.config.config_properties import get_ConfigProperties 

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "lufthansa_level"
SCHEMA = "bronze"
LANDING_VOLUME = "landing_area"
META_VOLUME = "autoloader_metadata"


# configProperties = get_ConfigProperties()

ENDPOINTKEYS_PIPELINE_LIST = [
    EndpointKeys.AIRPORTS,
    EndpointKeys.CITIES,
    EndpointKeys.COUNTRIES,
    EndpointKeys.AIRLINES,
    EndpointKeys.AIRCRAFT,
    EndpointKeys.FLIGHTSTATUS_BY_ROUTE,
    EndpointKeys.FLIGHT_SCHEDULES,
]

def build_source_path(endpoint_config: EndpointConfig) -> str:
    base_path = (
        f"/Volumes/{CATALOG}/"
        f"{SCHEMA}/"
        f"{LANDING_VOLUME}/"
        f"{endpoint_config.raw_folder}"
    )
    return base_path


def build_schema_location(endpoint_config: EndpointConfig) -> str:
    base_path = (
        f"/Volumes/{CATALOG}/"
        f"{SCHEMA}/"
        f"{META_VOLUME}/"
        f"{endpoint_config.raw_folder}/"
        f"schema"
    )
    return base_path

# TODO: time_period ?
def build_stream(endpoint_config: EndpointConfig, configProperties: ConfigProperties):
    source_path = build_source_path(endpoint_config)
    # checkpoint_location  = endpoint_config.build_checkpoint_location(configProperties.storage)
    schema_location = build_schema_location(endpoint_config)
    
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", schema_location)
        .load(source_path)
    )
        # .select(
        #     F.col("content").cast("string").alias("raw_json"),
        #     F.col("path").alias("source_file"),       
        #     F.col("modificationTime").alias("source_file_modification_time"),          
        #     F.current_timestamp().alias("bronze_ingested_at"),)
    df = (
        df
        .withColumn("raw_json", F.col("content").cast("string"))
        .withColumnRenamed("path", "source_file")
        .withColumn("bronze_ingested_at", F.current_timestamp())
        .drop("content", "length")
    )
    df = df.select(
        "bronze_ingested_at",
        "source_file",
        "modificationTime",
        *[col for col in df.columns if col not in {"bronze_ingested_at", "source_file", "modificationTime", "raw_json"}],
        "raw_json"
    )
    return df


def register_raw_table(endpoint_config: EndpointConfig, configProperties: ConfigProperties):
    @dp.table(name=endpoint_config.bronze_table)
    def _table():
        return build_stream(endpoint_config, configProperties)

    return _table


for endpoint_key in ENDPOINTKEYS_PIPELINE_LIST:
    endpoint_config = get_endpoint_config(endpoint_key)
    configProperties = get_ConfigProperties()
    register_raw_table(endpoint_config=endpoint_config, configProperties=configProperties)
