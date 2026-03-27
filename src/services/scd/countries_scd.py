from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower
import src.services.parsing_schemas as schemas

from src.services.scd.utils.rules import (
    apply_validations,
    COUNTRY_RULES,
    CITY_RULES,
    AIRPORT_RULES,
    AIRLINE_RULES,
    AIRCRAFT_RULES,
)


BRONZE_SOURCE = "lufthansa_level.bronze.countries_raw"
entity_alias="country"
code_field="CountryCode"
code_alias="country_code"
name_alias="country_name"
key_field="CountryCode"
key_alias="country_code"


@dp.view(
    comment="Entry point: parsing raw JSON from Bronze into Silver structured format"
)
def exploded_entity():
    return (
        dp.read_stream(BRONZE_SOURCE)
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("ingest_run_id"),
            from_json(col("raw_json"), schemas.country_resource_schema).alias("data_json")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("ingest_run_id"),
            explode(col("data_json.CountryResource.Countries.Country")).alias(entity_alias)
        )
        .select(
            "*",
            upper(trim(col(f"{entity_alias}.{code_field}"))).alias(code_alias)
        )
    )


@dp.view
@dp.expect_all_or_drop(COUNTRY_RULES["ref_country_names_flat"])
def array_names_flat_df():
    exploded_names = (
        dp.read_stream("exploded_entity")
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("ingest_run_id"),
            col(code_alias),
            explode(col(f"{entity_alias}.Names.Name")).alias("name")
        )
    )
    exploded_names =(
        exploded_names.select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("ingest_run_id"),
            col(code_alias),
            upper(trim(col("name.`@LanguageCode`"))).alias("language_code"),
            col("name.`$`").alias(name_alias),
        )
    )
    return exploded_names


@dp.view
@dp.expect_all_or_drop(COUNTRY_RULES["ref_dim_country"])
def simple_dim_df():
    extra_fields = None
    select_exprs = [
        col("source_file"),
        col("bronze_ingested_at"),
        col("ingest_run_id"),
        col(code_alias),
    ]

    if extra_fields:
        for source_field, target_alias in extra_fields.items():
            select_exprs.append(
                col(f"{entity_alias}.{source_field}").alias(target_alias)
            )

    df = dp.read_stream("exploded_entity").select(*select_exprs)
    return df


dp.create_streaming_table("ref_dim_country")

dp.create_auto_cdc_flow(
    target = "ref_dim_country",
    source = "simple_dim_df",
    keys = ["country_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 1
)


dp.create_streaming_table("ref_country_names_flat")

dp.create_auto_cdc_flow(
    target = "ref_country_names_flat",
    source = "array_names_flat_df",
    keys = ["country_code", "language_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 2,
    track_history_column_list = ["country_name"]
)
