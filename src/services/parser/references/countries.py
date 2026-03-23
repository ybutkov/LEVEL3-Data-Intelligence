from pyspark.sql.functions import col, explode

from src.services.parser.reference_orchestrator import run_reference_parser
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.app.logger import get_logger
from src.services.parser.utils.validation_utils import split_by_rules, build_quarantine_df
from pyspark.sql.functions import col, explode, length

logger = get_logger(__name__)


country_dim_rules = {
    "country_code": [
        lambda c: c.isNotNull(),
        lambda c: length(c) == 2,
    ]
}

country_names_rules = {
    "country_code": [
        lambda c: c.isNotNull(),
        lambda c: length(c) == 2,
    ],
    "language_code": [
        lambda c: c.isNotNull(),
        lambda c: length(c) == 2,
    ],
    "country_name": [
        lambda c: c.isNotNull(),
        lambda c: length(c) == 3,
    ],
}

def transform_countries(valid_df):
    logger.info("Start transform_countries")
    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.CountryResource.Countries.Country")).alias("country")
            )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("country.CountryCode").alias("country_code"),
            explode(col("country.Names.Name")).alias("name")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("country_code"),
            col("name.@LanguageCode").alias("language_code"),
            col("name.$").alias("country_name")
        )
    )

    logger.info("Finish transform_countries")
    return result_df


def build_ref_dim_country(valid_df):
    logger.info("Start build_ref_dim_country")

    result_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.CountryResource.Countries.Country")).alias("country")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("country.CountryCode").alias("country_code")
        )
        .dropDuplicates(["country_code"])
    )

    logger.info("Finish build_ref_dim_country")
    return result_df


# v1
# def build_country_outputs(valid_df):
#     country_names_flat_df = (
#         transform_countries(valid_df)
#         .dropDuplicates(["country_code", "language_code"])
#     )

#     ref_dim_country_df = build_ref_dim_country(valid_df)

#     return {
#         "ref_country_names_flat": country_names_flat_df,
#         "ref_dim_country": ref_dim_country_df,
#     }

def build_country_outputs(valid_df):
    country_names_flat_df = (
        transform_countries(valid_df)
        .dropDuplicates(["country_code", "language_code"])
    )

    ref_dim_country_df = build_ref_dim_country(valid_df)

    valid_dim_df, invalid_dim_df = split_by_rules(
        ref_dim_country_df,
        country_dim_rules,
    )

    valid_names_df, invalid_names_df = split_by_rules(
        country_names_flat_df,
        country_names_rules,
    )

    quarantine_dfs = []

    if invalid_dim_df.take(1):
        quarantine_dfs.append(
            build_quarantine_df(
                invalid_dim_df,
                dataset_name="countries",
                target_table="ref_dim_country",
                rule_name="country_dim_rules",
            )
        )

    if invalid_names_df.take(1):
        quarantine_dfs.append(
            build_quarantine_df(
                invalid_names_df,
                dataset_name="countries",
                target_table="ref_country_names_flat",
                rule_name="country_names_rules",
            )
        )

    outputs = {
        "ref_country_names_flat": valid_names_df,
        "ref_dim_country": valid_dim_df,
    }

    if quarantine_dfs:
        quarantine_df = quarantine_dfs[0]
        for qdf in quarantine_dfs[1:]:
            quarantine_df = quarantine_df.unionByName(qdf)

        outputs["audit_quarantine_records"] = quarantine_df

    return outputs


def run_countries(spark, cfg):
    run_reference_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.COUNTRIES,
        schema=schemas.country_resource_schema,
        build_outputs_fn=build_country_outputs,
    )
