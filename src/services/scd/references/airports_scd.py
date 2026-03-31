from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, expr
import src.services.parsing_schemas as schemas
from src.services.scd.utils.rules import AIRPORT_RULES
from src.services.scd.utils.json_normalization import normalize_names_field

AIRPORT_BRONZE_SOURCE = "lufthansa_level.bronze.airports_raw"
AIRPORT_META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias = "airport"
code_field = "AirportCode"
code_alias = "airport_code"
name_alias = "airport_name"


@dp.table(name="silver_audit.err_airport_invalid_json")
def airport_invalid_json():
    return (
        dp.read_stream(AIRPORT_BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.airport_resource_schema))
        .filter(col("parsed").isNull())
        .select(
            *AIRPORT_META_FIELDS,
            col("raw_json")
        )
    )


@dp.view(name="exploded_airport_entity")
def exploded_airport_entity():
    return (
        dp.read_stream(AIRPORT_BRONZE_SOURCE)
        .withColumn("raw_json_normalized", normalize_names_field(col("raw_json")))
        .select(
            *[col(f) for f in AIRPORT_META_FIELDS],
            from_json(col("raw_json_normalized"), schemas.airport_resource_schema).alias("data_json")
        )
        .filter(col("data_json").isNotNull())
        .select(
            *[col(f) for f in AIRPORT_META_FIELDS],
            explode(col("data_json.AirportResource.Airports.Airport")).alias(entity_alias)
        )
        .select(
            "*",
            upper(trim(col(f"{entity_alias}.{code_field}"))).alias(code_alias),
            upper(trim(col(f"{entity_alias}.CityCode"))).alias("city_code"),
            upper(trim(col(f"{entity_alias}.CountryCode"))).alias("country_code"),
            col(f"{entity_alias}.Position.Coordinate.Latitude").alias("latitude"),
            col(f"{entity_alias}.Position.Coordinate.Longitude").alias("longitude"),
            col(f"{entity_alias}.TimeZoneId").alias("time_zone_id"),
            col(f"{entity_alias}.UtcOffset").alias("utc_offset"),
            col(f"{entity_alias}.LocationType").alias("location_type")
        )
    )


@dp.view
def dim_airport_rules_checked():
    df = dp.read_stream("exploded_airport_entity")
    rules = AIRPORT_RULES["ref_dim_airport"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    dim_quarantine_rules = "NOT({0})".format(combined_condition)
    return df.withColumn("is_dim_quarantined", expr(dim_quarantine_rules))


@dp.view
def dim_airport_df():
    return (
        dp.read_stream("dim_airport_rules_checked")
        .filter("is_dim_quarantined=false")
        .select(
            *AIRPORT_META_FIELDS,
            col(code_alias),
            col("city_code"),
            col("country_code"),
            expr("st_point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE))").alias("coordinates"),
            col("time_zone_id"),
            col("utc_offset"),
            col("location_type"),
        )
    )


@dp.table(name="silver_audit.err_dim_airport_quarantine")
def dim_airport_quarantine():
    return (
        dp.read_stream("dim_airport_rules_checked")
        .filter("is_dim_quarantined=true")
        .select(
            *AIRPORT_META_FIELDS,
            col(code_alias),
            col("city_code"),
            col("country_code"),
            expr("st_point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE))").alias("coordinates"),
            col("time_zone_id"),
            col("utc_offset"),
            col("location_type"),
        )
    )


@dp.view
def airport_names_flat_checked():
    df = dp.read_stream("exploded_airport_entity")
    dim_rules = AIRPORT_RULES["ref_dim_airport"]
    dim_combined = " AND ".join([f"({cond})" for cond in dim_rules.values()])
    
    df = df.filter(dim_combined)
    
    rules = AIRPORT_RULES["ref_airport_names_flat"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    quarantine_name_rules = "NOT({0})".format(combined_condition)
    
    df = df.withColumn(
        "names_array",
        expr(f"coalesce({entity_alias}.Names.Name, array())")
    ).select(
        *[col(f) for f in AIRPORT_META_FIELDS],
        col(code_alias),
        explode(col("names_array")).alias("n")
    ).select(
        *[col(f) for f in AIRPORT_META_FIELDS],
        col(code_alias),
        upper(trim(col("n.`@LanguageCode`"))).alias("language_code"),
        col("n.$").alias(name_alias),
    )
    
    return df.withColumn("is_names_quarantined", expr(quarantine_name_rules))


@dp.view
def airport_names():
    return (
        dp.read_stream("airport_names_flat_checked")
        .filter("is_names_quarantined=false")
        .select(
            *AIRPORT_META_FIELDS,
            col(code_alias),
            col("language_code"),
            trim(col(name_alias)).alias(name_alias),
        )
    )


@dp.table(name="silver_audit.err_airport_names_quarantine")
def airport_names_quarantine():
    return (
        dp.read_stream("airport_names_flat_checked")
        .filter("is_names_quarantined=true")
        .select(
            *AIRPORT_META_FIELDS,
            col(code_alias),
            col("language_code"),
            trim(col(name_alias)).alias(name_alias),
        )
    )


dp.create_streaming_table("silver.ref_dim_airport")
dp.create_auto_cdc_flow(
    target="silver.ref_dim_airport",
    source="dim_airport_df",
    keys=["airport_code"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=1
)

dp.create_streaming_table("silver.ref_airport_names_flat")
dp.create_auto_cdc_flow(
    target="silver.ref_airport_names_flat",
    source="airport_names",
    keys=["airport_code", "language_code"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=2,
    track_history_column_list=["airport_name"]
)
