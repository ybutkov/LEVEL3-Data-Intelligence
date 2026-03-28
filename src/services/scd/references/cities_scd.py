from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower, expr
import src.services.parsing_schemas as schemas

from src.services.scd.utils.rules import CITY_RULES


CITY_BRONZE_SOURCE = "lufthansa_level.bronze.cities_raw"
CITY_META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias = "city"
code_field = "CityCode"
code_alias = "city_code"
name_alias = "city_name"


@dp.table(name="silver_audit.err_city_invalid_json")
def city_invalid_json():
    return (
        dp.read_stream(CITY_BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.city_resource_schema))
        .filter(col("parsed").isNull())
        .select(
            *CITY_META_FIELDS,
            col("raw_json")
        )
    )


@dp.view(name="exploded_city_entity")
def exploded_city_entity():
    return (
        dp.read_stream(CITY_BRONZE_SOURCE)
        .select(
            *[col(f) for f in CITY_META_FIELDS],
            from_json(col("raw_json"), schemas.city_resource_schema).alias("data_json")
        )
        .filter(col("data_json").isNotNull())
        .select(
            *[col(f) for f in CITY_META_FIELDS],
            explode(col("data_json.CityResource.Cities.City")).alias(entity_alias)
        )
        .select(
            "*",
            upper(trim(col(f"{entity_alias}.{code_field}"))).alias(code_alias),
            upper(trim(col(f"{entity_alias}.CountryCode"))).alias("country_code"),
            col(f"{entity_alias}.UtcOffset").alias("utc_offset"),
            col(f"{entity_alias}.TimeZoneId").alias("time_zone_id")
        )
    )


@dp.view
def dim_city_rules_checked():
    df = dp.read_stream("exploded_city_entity")
    rules = CITY_RULES["ref_dim_city"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    dim_quarantine_rules = "NOT({0})".format(combined_condition)
    return df.withColumn("is_dim_quarantined", expr(dim_quarantine_rules))


@dp.view
def dim_city_df():
    return (
        dp.read_stream("dim_city_rules_checked")
        .filter("is_dim_quarantined=false")
        .select(
            *CITY_META_FIELDS,
            col(code_alias),
            col("country_code"),
            col("utc_offset"),
            col("time_zone_id"),
        )
    )


@dp.table(name="silver_audit.err_dim_city_quarantine")
def dim_city_quarantine():
    return (
        dp.read_stream("dim_city_rules_checked")
        .filter("is_dim_quarantined=true")
        .select(
            *CITY_META_FIELDS,
            col(code_alias),
            col("country_code"),
            col("utc_offset"),
            col("time_zone_id"),
        )
    )


@dp.view
def city_names_flat_checked():
    df = dp.read_stream("dim_city_rules_checked").filter("is_dim_quarantined=false")
    rules = CITY_RULES["ref_city_names_flat"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    quarantine_name_rules = "NOT({0})".format(combined_condition)
    
    df = df.select(
        *[col(f) for f in CITY_META_FIELDS],
        col(code_alias),
        explode(col(f"{entity_alias}.Names.Name")).alias("n")
    ).select(
        *[col(f) for f in CITY_META_FIELDS],
        col(code_alias),
        upper(trim(col("n.`@LanguageCode`"))).alias("language_code"),
        col("n.$").alias(name_alias),
    )
    
    return df.withColumn("is_names_quarantined", expr(quarantine_name_rules))


@dp.view
def city_names():
    return (
        dp.read_stream("city_names_flat_checked")
        .filter("is_names_quarantined=false")
        .select(
            *CITY_META_FIELDS,
            col(code_alias),
            col("language_code"),
            col(name_alias),
        )
    )


@dp.table(name="silver_audit.err_city_names_quarantine")
def city_names_quarantine():
    return (
        dp.read_stream("city_names_flat_checked")
        .filter("is_names_quarantined=true")
        .select(
            *CITY_META_FIELDS,
            col(code_alias),
            col("language_code"),
            col(name_alias),
        )
    )


@dp.view
def airport_map_checked():
    df = dp.read_stream("exploded_city_entity")
    rules = CITY_RULES["ref_city_airport_map"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    quarantine_airport_rules = "NOT({0})".format(combined_condition)
    
    df = df.select(
        *[col(f) for f in CITY_META_FIELDS],
        col(code_alias),
        explode(col(f"{entity_alias}.Airports.AirportCode")).alias("airport_code")
    )
    
    return df.withColumn("is_airport_quarantined", expr(quarantine_airport_rules))


@dp.view
def city_airport_map():
    return (
        dp.read_stream("airport_map_checked")
        .filter("is_airport_quarantined=false")
        .select(
            *CITY_META_FIELDS,
            col(code_alias),
            col("airport_code"),
        )
    )


@dp.table(name="silver_audit.err_city_airport_map_quarantine")
def city_airport_map_quarantine():
    return (
        dp.read_stream("airport_map_checked")
        .filter("is_airport_quarantined=true")
        .select(
            *CITY_META_FIELDS,
            col(code_alias),
            col("airport_code"),
        )
    )


dp.create_streaming_table("silver.ref_dim_city")
dp.create_auto_cdc_flow(
    target="silver.ref_dim_city",
    source="dim_city_df",
    keys=[code_alias],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=1
)


dp.create_streaming_table("silver.ref_city_names_flat")
dp.create_auto_cdc_flow(
    target="silver.ref_city_names_flat",
    source="city_names",
    keys=[code_alias, "language_code"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=2,
    track_history_column_list=[name_alias]
)


dp.create_streaming_table("silver.ref_city_airport_map")
dp.create_auto_cdc_flow(
    target="silver.ref_city_airport_map",
    source="city_airport_map",
    keys=[code_alias, "airport_code"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=1
)
