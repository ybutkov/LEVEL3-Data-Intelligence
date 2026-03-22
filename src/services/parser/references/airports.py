from pyspark.sql.functions import col, explode

from src.services.parser.reference_orchestrator import run_reference_parser
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.app.logger import get_logger


logger = get_logger(__name__)


def transform_airport_names(valid_df):
    logger.info("Start transform_airport_names")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.AirportResource.Airports.Airport")).alias("airport")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("airport.AirportCode").alias("airport_code"),
            explode(col("airport.Names.Name")).alias("name")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("airport_code"),
            col("name.@LanguageCode").alias("language_code"),
            col("name.$").alias("airport_name")
        )
    )

    logger.info("Finish transform_airport_names")
    return result_df


def build_ref_dim_airport(valid_df):
    logger.info("Start build_ref_dim_airport")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.AirportResource.Airports.Airport")).alias("airport")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("airport.AirportCode").alias("airport_code"),
            col("airport.CityCode").alias("city_code"),
            col("airport.CountryCode").alias("country_code"),
            col("airport.Position.Coordinate.Latitude").cast("double").alias("latitude"),
            col("airport.Position.Coordinate.Longitude").cast("double").alias("longitude"),
            col("airport.UtcOffset").alias("utc_offset"),
            col("airport.TimeZoneId").alias("time_zone_id"),
        )
        .dropDuplicates(["airport_code"])
    )

    logger.info("Finish build_ref_dim_airport")
    return result_df


def build_airport_outputs(valid_df):
    airport_names_flat_df = (
        transform_airport_names(valid_df)
        .dropDuplicates(["airport_code", "language_code"])
    )

    ref_dim_airport_df = build_ref_dim_airport(valid_df)

    return {
        "ref_airport_names_flat": airport_names_flat_df,
        "ref_dim_airport": ref_dim_airport_df,
    }


def run_airports(spark, cfg):
    run_reference_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.AIRPORTS,
        schema=schemas.airport_resource_schema,
        build_outputs_fn=build_airport_outputs,
    )