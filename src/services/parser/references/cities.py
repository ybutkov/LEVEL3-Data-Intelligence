from pyspark.sql.functions import col, explode

from src.services.parser.reference_orchestrator import run_reference_parser
# from src.services.parser.utils.normalization import normalize_string_columns
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.app.logger import get_logger


logger = get_logger(__name__)


def transform_city_names(valid_df):
    logger.info("Start transform_city_names")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.CityResource.Cities.City")).alias("city")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("city.CityCode").alias("city_code"),
            explode(col("city.Names.Name")).alias("name")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("city_code"),
            col("name.@LanguageCode").alias("language_code"),
            col("name.$").alias("city_name")
        )
    )

    logger.info("Finish transform_city_names")
    return result_df


def build_ref_dim_city(valid_df):
    logger.info("Start build_ref_dim_city")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.CityResource.Cities.City")).alias("city")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("city.CityCode").alias("city_code"),
            col("city.CountryCode").alias("country_code"),
            col("city.UtcOffset").alias("utc_offset"),
            col("city.TimeZoneId").alias("time_zone_id"),
        )
        .dropDuplicates(["city_code"])
    )

    logger.info("Finish build_ref_dim_city")
    return result_df


def build_ref_city_airport_map(valid_df):
    logger.info("Start build_ref_city_airport_map")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.CityResource.Cities.City")).alias("city")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("city.CityCode").alias("city_code"),
            explode(col("city.Airports.AirportCode")).alias("airport_code")
        )
        .dropDuplicates(["city_code", "airport_code"])
    )

    logger.info("Finish build_ref_city_airport_map")
    return result_df


def build_city_outputs(valid_df):
    city_names_flat_df = (
        transform_city_names(valid_df)
        # .transform(lambda df: normalize_string_columns(df, ["city_code", "language_code"]))
        .dropDuplicates(["city_code", "language_code"])
    )

    ref_dim_city_df = build_ref_dim_city(valid_df)
    # ref_dim_city_df = build_ref_dim_city(valid_df).transform(
    #     lambda df: normalize_string_columns(df, ["city_code", "language_code"])
    # )

    ref_city_airport_map_df = build_ref_city_airport_map(valid_df)
    # ref_city_airport_map_df = build_ref_city_airport_map(valid_df).transform(
    #     lambda df: normalize_string_columns(df, ["city_code", "language_code"])
    # )

    return {
        "ref_city_names_flat": city_names_flat_df,
        "ref_dim_city": ref_dim_city_df,
        "ref_city_airport_map": ref_city_airport_map_df,
    }


def run_cities(spark, cfg):
    run_reference_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.CITIES,
        schema=schemas.city_resource_schema,
        build_outputs_fn=build_city_outputs,
    )