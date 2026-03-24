from src.services.parser.parser_orchestrator import run_parser
from src.services.parser.utils.reference_builders import (
    explode_entity,
    build_single_names_flat_df,
    build_simple_dim_df,
)
from src.services.parser.references.reference_rules import REFERENCE_RULES
from src.services.parser.utils.normalize_utils import REFERENCE_NORMALIZATION_MAP
from src.services.parser.references.reference_transformations import REFERENCE_TRANSFORMATION_MAP
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.app.logger import get_logger


logger = get_logger(__name__)


def build_airline_outputs(valid_df):
    logger.info("Start build_airline_outputs")

    exploded_df = explode_entity(
        valid_df=valid_df,
        entity_path="data_json.AirlineResource.Airlines.Airline",
        entity_alias="airline"
    )

    logger.info("Start ref_airline_names_flat")
    ref_airline_names_flat_df = (
        build_single_names_flat_df(
            exploded_df=exploded_df,
            entity_alias="airline",
            code_field="AirlineID",
            code_alias="airline_id",
            name_alias="airline_name",
        )
        .dropDuplicates(["airline_id", "language_code"])
    )

    logger.info("Start ref_dim_airline")
    ref_dim_airline_df = build_simple_dim_df(
        exploded_df=exploded_df,
        entity_alias="airline",
        key_field="AirlineID",
        key_alias="airline_id",
        extra_fields={
            "AirlineID_ICAO": "airline_id_icao",
        },
    )

    logger.info("Finish build_airline_outputs")

    return {
        "ref_airline_names_flat": ref_airline_names_flat_df,
        "ref_dim_airline": ref_dim_airline_df,
    }


def run_airlines(spark, cfg):
    run_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.AIRLINES,
        schema=schemas.airline_resource_schema,
        build_outputs_fn=build_airline_outputs,
        normalization_map=REFERENCE_NORMALIZATION_MAP,
        transformation_map=REFERENCE_TRANSFORMATION_MAP,
        rules_map=REFERENCE_RULES
    )