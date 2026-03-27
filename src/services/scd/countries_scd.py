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
def array_names_flat_raw():
    return (
        dp.read_stream("exploded_entity")
        .select(
            "source_file",
            "bronze_ingested_at",
            "ingest_run_id",
            col(code_alias),
            explode(col(f"{entity_alias}.Names.Name")).alias("n")
        )
        .select(
            "source_file",
            "bronze_ingested_at",
            "ingest_run_id",
            col(code_alias),
            lower(trim(col("n.`@LanguageCode`"))).alias("language_code"),
            col("n.$").alias(name_alias)
        )
    )


@dp.table(name="ref_country_names_flat_validated")
@dp.expect_all_or_drop(COUNTRY_RULES["ref_country_names_flat"])
def array_names_flat_df():
    return dp.read_stream("array_names_flat_raw")


@dp.table(name="silver_audit.err_country_invalid_json")
def invalid_json():
    return (
        dp.read_stream(BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.country_resource_schema))
        .filter(col("parsed").isNull())
        .select("source_file", "bronze_ingested_at", "ingest_run_id", "raw_json")
    )


@dp.table(name="silver_audit.err_country_quarantine")
def country_quarantine():
    df = dp.read_stream("array_names_flat_raw")
    rules = COUNTRY_RULES["ref_country_names_flat"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    return df.filter(f"NOT ({combined_condition})")

@dp.view
@dp.expect_all_or_drop(COUNTRY_RULES["ref_dim_country"])
def simple_dim_df():
    return dp.read_stream("exploded_entity").select(
        "source_file", "bronze_ingested_at", "ingest_run_id", col(code_alias)
    )

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
    source = "ref_country_names_flat_validated",
    keys = ["country_code", "language_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 2,
    track_history_column_list = ["country_name"]
)
