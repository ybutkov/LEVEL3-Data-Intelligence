from pyspark.sql.functions import col, explode

from src.services.parser.reference_orchestrator import run_reference_parser
# from src.services.parser.utils.normalization import normalize_string_columns
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.app.logger import get_logger


logger = get_logger(__name__)


def transform_aircraft_names(valid_df):
    logger.info("Start transform_aircraft_names")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(
                col("data_json.AircraftResource.AircraftSummaries.AircraftSummary")
            ).alias("aircraft")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("aircraft.AircraftCode").alias("aircraft_code"),
            col("aircraft.Names.Name.@LanguageCode").alias("language_code"),
            col("aircraft.Names.Name.$").alias("aircraft_name"),
        )
    )

    logger.info("Finish transform_aircraft_names")
    return result_df


def build_ref_dim_aircraft(valid_df):
    logger.info("Start build_ref_dim_aircraft")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(
                col("data_json.AircraftResource.AircraftSummaries.AircraftSummary")
            ).alias("aircraft")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("aircraft.AircraftCode").alias("aircraft_code"),
            col("aircraft.AirlineEquipCode").alias("airline_equip_code"),
        )
        .dropDuplicates(["aircraft_code"])
    )

    logger.info("Finish build_ref_dim_aircraft")
    return result_df


def build_aircraft_outputs(valid_df):
    ref_aircraft_names_flat_df = (
        transform_aircraft_names(valid_df)
            # .transform(lambda df: normalize_string_columns(df, ["aircraft_code", "language_code"]))
            .dropDuplicates(["aircraft_code", "language_code"])
    )

    ref_dim_aircraft_df = build_ref_dim_aircraft(valid_df)
    # ref_dim_aircraft_df = build_ref_dim_aircraft(valid_df).transform(
    #     lambda df: normalize_string_columns(df, ["aircraft_code", "airline_equip_code"])
    # )

    return {
        "ref_aircraft_names_flat": ref_aircraft_names_flat_df,
        "ref_dim_aircraft": ref_dim_aircraft_df,
    }


def run_aircraft(spark, cfg):
    run_reference_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.AIRCRAFT,
        schema=schemas.aircraft_resource_schema,
        build_outputs_fn=build_aircraft_outputs,
    )
    