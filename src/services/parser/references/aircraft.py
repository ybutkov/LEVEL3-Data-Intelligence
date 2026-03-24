from src.services.parser.reference_orchestrator import run_reference_parser
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas

from src.services.parser.utils.reference_builders import (
    explode_entity,
    build_single_names_flat_df,
    build_simple_dim_df,
)
from src.app.logger import get_logger


logger = get_logger(__name__)


def build_aircraft_outputs(valid_df):

    logger.info("Start build_aircraft_outputs")
    exploded_df = explode_entity(
        valid_df=valid_df,
        entity_path="data_json.AircraftResource.AircraftSummaries.AircraftSummary",
        entity_alias="aircraft"
    )

    logger.info("Start build_single_names_flat")
    ref_aircraft_names_flat = (
        build_single_names_flat_df(
            exploded_df=exploded_df,
            entity_alias="aircraft",
            code_field="AircraftCode",
            code_alias="aircraft_code",
            name_alias="aircraft_name",
        )
        .dropDuplicates(["aircraft_code", "language_code"])
    )

    logger.info("Start ref_dim_aircraft")
    ref_dim_aircraft = build_simple_dim_df(
        exploded_df=exploded_df,
        entity_alias="aircraft",
        key_field="AircraftCode",
        key_alias="aircraft_code",
        extra_fields={
            "AirlineEquipCode": "airline_equip_code",
        },
    )
    logger.info("Finish build_aircraft_outputs")

    return {
        "ref_aircraft_names_flat": ref_aircraft_names_flat,
        "ref_dim_aircraft": ref_dim_aircraft,
    }

def run_aircraft(spark, cfg):
    run_reference_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.AIRCRAFT,
        schema=schemas.aircraft_resource_schema,
        build_outputs_fn=build_aircraft_outputs,
    )
