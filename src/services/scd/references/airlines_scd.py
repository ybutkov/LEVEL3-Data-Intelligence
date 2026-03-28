from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower, lit, expr
import src.services.parsing_schemas as schemas
from src.services.scd.utils.rules import AIRLINE_RULES


AIRLINE_BRONZE_SOURCE = "lufthansa_level.bronze.airlines_raw"
AIRLINE_META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias = "airline"
code_field = "AirlineID"
code_alias = "airline_id"
name_alias = "airline_name"


@dp.table(name="silver_audit.err_airline_invalid_json")
def airline_invalid_json():
    return (
        dp.read_stream(AIRLINE_BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.airline_resource_schema))
        .filter(col("parsed").isNull())
        .select(
            *AIRLINE_META_FIELDS,
            col("raw_json")
        )
    )


@dp.view(name="exploded_airline_entity")
def exploded_airline_entity():
    return (
        dp.read_stream(AIRLINE_BRONZE_SOURCE)
        .select(
            *[col(f) for f in AIRLINE_META_FIELDS],
            from_json(col("raw_json"), schemas.airline_resource_schema).alias("data_json")
        )
        .filter(col("data_json").isNotNull())
        .select(
            *[col(f) for f in AIRLINE_META_FIELDS],
            explode(col("data_json.AirlineResource.Airlines.Airline")).alias(entity_alias)
        )
        .select(
            "*",
            upper(trim(col(f"{entity_alias}.{code_field}"))).alias(code_alias),
            upper(trim(col(f"{entity_alias}.AirlineID_ICAO"))).alias("icao_code")
        )
    )


@dp.view
def dim_airline_rules_checked():
    df = dp.read_stream("exploded_airline_entity")
    rules = AIRLINE_RULES["ref_dim_airline"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    dim_quarantine_rules = "NOT({0})".format(combined_condition)
    return df.withColumn("is_dim_quarantined", expr(dim_quarantine_rules))


@dp.view
def dim_airline_df():
    return (
        dp.read_stream("dim_airline_rules_checked")
        .filter("is_dim_quarantined=false")
        .select(
            *AIRLINE_META_FIELDS,
            col(code_alias),
            col("icao_code"),
        )
    )


@dp.table(name="silver_audit.err_dim_airline_quarantine")
def dim_airline_quarantine():
    return (
        dp.read_stream("dim_airline_rules_checked")
        .filter("is_dim_quarantined=true")
        .select(
            *AIRLINE_META_FIELDS,
            col(code_alias),
            col("icao_code"),
        )
    )


@dp.view
def airline_names_flat_checked():
    df = dp.read_stream("exploded_airline_entity")
    dim_rules = AIRLINE_RULES["ref_dim_airline"]
    dim_combined = " AND ".join([f"({cond})" for cond in dim_rules.values()])
    
    df = df.filter(dim_combined)
    
    rules = AIRLINE_RULES["ref_airline_names_flat"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    quarantine_name_rules = "NOT({0})".format(combined_condition)
    
    df = df.withColumn(
        "names_array",
        expr(f"coalesce({entity_alias}.Names.Name, array())")
    ).select(
        *[col(f) for f in AIRLINE_META_FIELDS],
        col(code_alias),
        col("icao_code"),
        explode(col("names_array")).alias("n")
    ).select(
        *[col(f) for f in AIRLINE_META_FIELDS],
        col(code_alias),
        col("icao_code"),
        upper(trim(col("n.`@LanguageCode`"))).alias("language_code"),
        col("n.$").alias(name_alias),
    )
    
    return df.withColumn("is_names_quarantined", expr(quarantine_name_rules))


@dp.view
def airline_names():
    return (
        dp.read_stream("airline_names_flat_checked")
        .filter("is_names_quarantined=false")
        .select(
            *AIRLINE_META_FIELDS,
            col(code_alias),
            col("icao_code"),
            col("language_code"),
            col(name_alias),
        )
    )


@dp.table(name="silver_audit.err_airline_names_quarantine")
def airline_names_quarantine():
    return (
        dp.read_stream("airline_names_flat_checked")
        .filter("is_names_quarantined=true")
        .select(
            *AIRLINE_META_FIELDS,
            col(code_alias),
            col("icao_code"),
            col("language_code"),
            col(name_alias),    df = df.withColumn(

        )
    )


dp.create_streaming_table("silver.ref_dim_airline")
dp.create_auto_cdc_flow(
    target="silver.ref_dim_airline",
    source="dim_airline_df",
    keys=[code_alias, "icao_code"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=1
)


dp.create_streaming_table("silver.ref_airline_names_flat")
dp.create_auto_cdc_flow(
    target="silver.ref_airline_names_flat",
    source="airline_names",
    keys=[code_alias, "icao_code", "language_code"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=2,
    track_history_column_list=[name_alias]
)
