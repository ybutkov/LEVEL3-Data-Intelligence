from pyspark.sql.functions import col, explode

from src.services.parser.parser_orchestrator import run_parser
from src.services.parser.utils.reference_builders import (
    explode_entity,
    build_array_names_flat_df,
    build_simple_dim_df,
)
from src.services.parser.references.reference_rules import REFERENCE_RULES
from src.services.parser.utils.normalize_utils import REFERENCE_NORMALIZATION_MAP
from src.services.parser.references.reference_transformations import REFERENCE_TRANSFORMATION_MAP
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.app.logger import get_logger


logger = get_logger(__name__)


def build_ref_city_airport_map(exploded_df):

    result_df = (
        exploded_df
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("city.CityCode").alias("city_code"),
            explode(col("city.Airports.AirportCode")).alias("airport_code")
        )
        .dropDuplicates(["city_code", "airport_code"])
    )

    return result_df


def build_city_outputs(valid_df):
    logger.info("Start build_city_outputs")

    exploded_df = explode_entity(
        valid_df=valid_df,
        entity_path="data_json.CityResource.Cities.City",
        entity_alias="city"
    )

    logger.info("Start build_array_names_flat")
    ref_city_names_flat_df = (
        build_array_names_flat_df(
            exploded_df=exploded_df,
            entity_alias="city",
            code_field="CityCode",
            code_alias="city_code",
            name_alias="city_name",
        )
        .dropDuplicates(["city_code", "language_code"])
    )

    logger.info("Start build_simple_dim")
    ref_dim_city_df = build_simple_dim_df(
        exploded_df=exploded_df,
        entity_alias="city",
        key_field="CityCode",
        key_alias="city_code",
        extra_fields={
            "CountryCode": "country_code",
            "UtcOffset": "utc_offset",
            "TimeZoneId": "time_zone_id",
        },
    )

    logger.info("Start build_ref_city_airport_map")
    ref_city_airport_map_df = build_ref_city_airport_map(exploded_df)

    logger.info("Finish build_city_outputs")

    return {
        "ref_city_names_flat": ref_city_names_flat_df,
        "ref_dim_city": ref_dim_city_df,
        "ref_city_airport_map": ref_city_airport_map_df,
    }


def run_cities(spark, cfg):
    run_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.CITIES,
        schema=schemas.city_resource_schema,
        build_outputs_fn=build_city_outputs,
        normalization_map=REFERENCE_NORMALIZATION_MAP,
        transformation_map=REFERENCE_TRANSFORMATION_MAP,
        rules_map=REFERENCE_RULES
    )
