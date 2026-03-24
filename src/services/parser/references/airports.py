from src.services.parser.reference_orchestrator import run_reference_parser
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas

from src.services.parser.utils.reference_builders import (
    explode_entity,
    build_array_names_flat_df,
    build_simple_dim_df,
)
from src.app.logger import get_logger


logger = get_logger(__name__)


def build_airport_outputs(valid_df):

    logger.info("Start build_airport_outputs")
    exploded_df = explode_entity(
        valid_df=valid_df,
        entity_path="data_json.AirportResource.Airports.Airport",
        entity_alias="airport"
    )

    logger.info("Start build_single_names_flat")
    ref_airport_names_flat = (
        build_array_names_flat_df(
            exploded_df=exploded_df,
            entity_alias="airport",
            code_field="AirportCode",
            code_alias="airport_code",
            name_alias="airport_name",
        )
        .dropDuplicates(["airport_code", "language_code"])
    )

    logger.info("Start ref_dim_airport")
    ref_dim_airport = build_simple_dim_df(
        exploded_df=exploded_df,
        entity_alias="airport",
        key_field="AirportCode",
        key_alias="airport_code",
        extra_fields={
            "CityCode": "city_code",
            "CountryCode": "country_code",
            "Position.Coordinate.Latitude": "latitude",
            "Position.Coordinate.Longitude": "longitude",
            "UtcOffset": "utc_offset",
            "TimeZoneId": "time_zone_id",
        },
    )
    logger.info("Finish build_airport_outputs")

    return {
        "ref_airport_names_flat": ref_airport_names_flat,
        "ref_dim_airport": ref_dim_airport,
    }


def run_airports(spark, cfg):
    run_reference_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.AIRPORTS,
        schema=schemas.airport_resource_schema,
        build_outputs_fn=build_airport_outputs,
    )
