from pyspark.sql.functions import col, explode

from src.services.parser.reference_orchestrator import run_reference_parser
# from src.services.parser.utils.normalization import normalize_string_columns
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.app.logger import get_logger


logger = get_logger(__name__)


def transform_airline_names(valid_df):
    logger.info("Start transform_airline_names")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.AirlineResource.Airlines.Airline")).alias("airline")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("airline.AirlineID").alias("airline_id"),
            col("airline.Names.Name").alias("name")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("airline_id"),
            col("name.@LanguageCode").alias("language_code"),
            col("name.$").alias("airline_name"),
        )
    )

    logger.info("Finish transform_airline_names")
    return result_df


def build_ref_dim_airline(valid_df):
    logger.info("Start build_ref_dim_airline")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.AirlineResource.Airlines.Airline")).alias("airline")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("airline.AirlineID").alias("airline_id"),
            col("airline.AirlineID_ICAO").alias("airline_id_icao"),
        )
        .dropDuplicates(["airline_id"])
    )

    logger.info("Finish build_ref_dim_airline")
    return result_df


def build_airline_outputs(valid_df):
    ref_airline_names_flat_df = (
        transform_airline_names(valid_df)
        # .transform(lambda df: normalize_string_columns(df, ["airline_id", "language_code"]))
        .dropDuplicates(["airline_id", "language_code"])
    )

    ref_dim_airline_df = build_ref_dim_airline(valid_df)
    # ref_dim_airline_df = build_ref_dim_airline(valid_df).transform(
    #     lambda df: normalize_string_columns(df, ["airline_id", "airline_id_icao"])
    # )
    return {
        "ref_airline_names_flat": ref_airline_names_flat_df,
        "ref_dim_airline": ref_dim_airline_df,
    }


def run_airlines(spark, cfg):
    run_reference_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.AIRLINES,
        schema=schemas.airline_resource_schema,
        build_outputs_fn=build_airline_outputs,
    )