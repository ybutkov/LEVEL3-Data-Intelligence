from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower
import src.services.parsing_schemas as schemas

from src.services.scd.utils.rules import AIRCRAFT_RULES


AIRCRAFT_BRONZE_SOURCE = "lufthansa_level.bronze.aircraft_raw"
AIRCRAFT_META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias="aircraft"
code_field="AircraftCode"
code_alias="aircraft_code"
name_alias="aircraft_name"


@dp.view(
    name="exploded_aircraft_entity",
    comment="Entry point: parsing raw JSON from Bronze into Silver structured format"
)
def exploded_aircraft_entity():
    return (
        dp.read_stream(AIRCRAFT_BRONZE_SOURCE)
        .select(
                *[col(f) for f in AIRCRAFT_META_FIELDS],
            from_json(col("raw_json"), schemas.aircraft_resource_schema).alias("data_json")
        )
        .select(
            *[col(f) for f in AIRCRAFT_META_FIELDS],
            explode(col("data_json.AircraftResource.AircraftSummaries.AircraftSummary")).alias(entity_alias)
        )
        .select(
            "*",
            upper(trim(col(f"{entity_alias}.{code_field}"))).alias(code_alias)
        )
    )

@dp.table(name="valid_aircraft_names_flat")
@dp.expect_all_or_drop(AIRCRAFT_RULES["ref_aircraft_names_flat"])
def array_aircraft_names_flat_df():
    return (
        dp.read_stream("exploded_aircraft_entity")
        .select(
            *[col(f) for f in AIRCRAFT_META_FIELDS],
            col(f"{entity_alias}.{code_field}").alias(code_alias),
            col(f"{entity_alias}.Names.Name.@LanguageCode").alias("language_code"),
            col(f"{entity_alias}.Names.Name.$").alias(name_alias),
        )
        .select(
            *[col(f) for f in AIRCRAFT_META_FIELDS],
            upper(trim(col(code_alias))).alias(code_alias),
            upper(trim(col("language_code"))).alias("language_code"),
            col(name_alias)
        )
    )
    

@dp.table(name="silver_audit.err_aircraft_invalid_json")
def invalid_json():
    return (
        dp.read_stream(AIRCRAFT_BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.aircraft_resource_schema))
        .filter(col("parsed").isNull())
        .select(*AIRCRAFT_META_FIELDS, "raw_json")
    )


@dp.table(name="silver_audit.err_aircraft_quarantine")
def aircraft_quarantine():
    df = dp.read_stream("array_aircraft_names_flat_df")
    rules = AIRCRAFT_RULES["ref_aircraft_names_flat"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    return df.filter(f"NOT ({combined_condition})")


@dp.view(
    name="aircraft_dim_df"
)
@dp.expect_all_or_drop(AIRCRAFT_RULES["ref_dim_aircraft"])
def aircraft_dim_df():
    extra_fields={
            "AirlineEquipCode": "airline_equip_code",
        }
    select_exprs = [
        *[col(f) for f in AIRCRAFT_META_FIELDS],
        col(code_alias),
    ]

    if extra_fields:
        for source_field, target_alias in extra_fields.items():
            select_exprs.append(
                col(f"{entity_alias}.{source_field}").alias(target_alias)
            )

    df = dp.read_stream("exploded_aircraft_entity").select(*select_exprs)
    return df


dp.create_streaming_table("silver.ref_dim_aircraft")
dp.create_auto_cdc_flow(
    target = "silver.ref_dim_aircraft",
    source = "aircraft_dim_df",
    keys = ["aircraft_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 1
)


dp.create_streaming_table("silver.ref_aircraft_names_flat")
dp.create_auto_cdc_flow(
    target = "silver.ref_aircraft_names_flat",
    source = "valid_aircraft_names_flat",
    keys = ["aircraft_code", "language_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 2,
    track_history_column_list = ["aircraft_name"]
)
