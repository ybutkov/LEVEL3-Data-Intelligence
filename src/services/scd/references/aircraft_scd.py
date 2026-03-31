from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower, expr
import src.services.parsing_schemas as schemas
from src.services.scd.utils.rules import AIRCRAFT_RULES


"""
Aircraft SCD (Slowly Changing Dimension) Pipeline.

Processes aircraft reference data through silver layer:
1. Loads raw JSON from bronze
2. Explodes nested aircraft entity arrays
3. Validates data against aircraft rules
4. Applies SCD Type 2 logic (maintains history)
5. Outputs clean aircraft dimension table
"""

AIRCRAFT_BRONZE_SOURCE = "lufthansa_level.bronze.aircraft_raw"
AIRCRAFT_META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias = "aircraft"
code_field = "AircraftCode"
code_alias = "aircraft_code"
name_alias = "aircraft_name"


@dp.table(name="silver_audit.err_aircraft_invalid_json")
def aircraft_invalid_json():
    """
    Capture aircraft records with invalid JSON that failed parsing.
    
    Outputs to audit table for error tracking and investigation.
    """
    return (
        dp.read_stream(AIRCRAFT_BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.aircraft_resource_schema))
        .filter(col("parsed").isNull())
        .select(
            *AIRCRAFT_META_FIELDS,
            col("raw_json")
        )
    )


@dp.view(name="exploded_aircraft_entity")
def exploded_aircraft_entity():
    """
    Extract and explode aircraft entities from nested JSON structure.
    
    Parses raw JSON and flattens aircraft array for downstream processing.
    """
    return (
        dp.read_stream(AIRCRAFT_BRONZE_SOURCE)
        .select(
            *[col(f) for f in AIRCRAFT_META_FIELDS],
            from_json(col("raw_json"), schemas.aircraft_resource_schema).alias("data_json")
        )
        .filter(col("data_json").isNotNull())
        .select(
            *[col(f) for f in AIRCRAFT_META_FIELDS],
            explode(col("data_json.AircraftResource.AircraftSummaries.AircraftSummary")).alias(entity_alias)
        )
        .select(
            "*",
            upper(trim(col(f"{entity_alias}.{code_field}"))).alias(code_alias),
            upper(trim(col(f"{entity_alias}.AirlineEquipCode"))).alias("airline_equip_code")
        )
    )


@dp.view
def dim_aircraft_rules_checked():
    df = dp.read_stream("exploded_aircraft_entity")
    rules = AIRCRAFT_RULES["ref_dim_aircraft"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    dim_quarantine_rules = "NOT({0})".format(combined_condition)
    return df.withColumn("is_dim_quarantined", expr(dim_quarantine_rules))


@dp.view
def dim_aircraft_df():
    return (
        dp.read_stream("dim_aircraft_rules_checked")
        .filter("is_dim_quarantined=false")
        .select(
            *AIRCRAFT_META_FIELDS,
            col(code_alias),
            col("airline_equip_code"),
        )
    )


@dp.table(name="silver_audit.err_dim_aircraft_quarantine")
def dim_aircraft_quarantine():
    return (
        dp.read_stream("dim_aircraft_rules_checked")
        .filter("is_dim_quarantined=true")
        .select(
            *AIRCRAFT_META_FIELDS,
            col(code_alias),
            col("airline_equip_code"),
        )
    )


@dp.view
def aircraft_names_flat_checked():
    df = dp.read_stream("exploded_aircraft_entity")
    dim_rules = AIRCRAFT_RULES["ref_dim_aircraft"]
    dim_combined = " AND ".join([f"({cond})" for cond in dim_rules.values()])
    
    df = df.filter(dim_combined)

    rules = AIRCRAFT_RULES["ref_aircraft_names_flat"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    quarantine_name_rules = "NOT({0})".format(combined_condition)
    
    df = df.withColumn(
        "names_array",
        expr(f"coalesce({entity_alias}.Names.Name, array())")
    ).select(
        *[col(f) for f in AIRCRAFT_META_FIELDS],
        col(code_alias),
        explode(col("names_array")).alias("n")
    ).select(
        *[col(f) for f in AIRCRAFT_META_FIELDS],
        col(code_alias),
        upper(trim(col("n.`@LanguageCode`"))).alias("language_code"),
        col("n.$").alias(name_alias),
    )
    
    return df.withColumn("is_names_quarantined", expr(quarantine_name_rules))


@dp.view
def aircraft_names():
    return (
        dp.read_stream("aircraft_names_flat_checked")
        .filter("is_names_quarantined=false")
        .select(
            *AIRCRAFT_META_FIELDS,
            col(code_alias),
            col("language_code"),
            trim(col(name_alias)).alias(name_alias),
        )
    )


@dp.table(name="silver_audit.err_aircraft_names_quarantine")
def aircraft_names_quarantine():
    return (
        dp.read_stream("aircraft_names_flat_checked")
        .filter("is_names_quarantined=true")
        .select(
            *AIRCRAFT_META_FIELDS,
            col(code_alias),
            col("language_code"),
            trim(col(name_alias)).alias(name_alias),
        )
    )


dp.create_streaming_table("silver.ref_dim_aircraft")
dp.create_auto_cdc_flow(
    target="silver.ref_dim_aircraft",
    source="dim_aircraft_df",
    keys=["aircraft_code"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=1
)

dp.create_streaming_table("silver.ref_aircraft_names_flat")
dp.create_auto_cdc_flow(
    target="silver.ref_aircraft_names_flat",
    source="aircraft_names",
    keys=["aircraft_code", "language_code"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=2,
    track_history_column_list=["aircraft_name"]
)
