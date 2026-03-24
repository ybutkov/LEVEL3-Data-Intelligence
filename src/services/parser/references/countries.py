from src.services.parser.parser_orchestrator import run_parser
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.services.parser.references.reference_rules import REFERENCE_RULES
from src.services.parser.utils.normalize_utils import REFERENCE_NORMALIZATION_MAP
from src.services.parser.references.reference_transformations import REFERENCE_TRANSFORMATION_MAP
from src.services.parser.utils.reference_builders import (
    explode_entity,
    build_array_names_flat_df,
    build_simple_dim_df,
)
from src.app.logger import get_logger


logger = get_logger(__name__)


def build_country_outputs(valid_df):
    
    logger.info("Start build_countries_outputs")
    exploded_df = explode_entity(
        valid_df=valid_df,
        entity_path="data_json.CountryResource.Countries.Country",
        entity_alias="country"
    )

    logger.info("Start build_array_names_flat")
    ref_country_names_flat = (
        build_array_names_flat_df(
            exploded_df=exploded_df,
            entity_alias="country",
            code_field="CountryCode",
            code_alias="country_code",
            name_alias="country_name",
        )
        .dropDuplicates(["country_code", "language_code"])
    )

    logger.info("Start ref_dim_country")
    ref_dim_country = build_simple_dim_df(
        exploded_df=exploded_df,
        entity_alias="country",
        key_field="CountryCode",
        key_alias="country_code",
    )
    logger.info("Finish build_countries_outputs")

    return {
        "ref_country_names_flat": ref_country_names_flat,
        "ref_dim_country": ref_dim_country,
    }

def run_countries(spark, cfg):
    run_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.COUNTRIES,
        schema=schemas.country_resource_schema,
        build_outputs_fn=build_country_outputs,
        normalization_map=REFERENCE_NORMALIZATION_MAP,
        transformation_map=REFERENCE_TRANSFORMATION_MAP,
        rules_map=REFERENCE_RULES
    )
