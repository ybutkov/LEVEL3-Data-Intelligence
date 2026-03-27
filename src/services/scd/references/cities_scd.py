from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower
import src.services.parsing_schemas as schemas

from src.services.scd.utils.rules import CITY_RULES


CITY_BRONZE_SOURCE = "lufthansa_level.bronze.cities_raw"
CITY_META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias="city"
code_field="CityCode"
code_alias="city_code"
name_alias="city_name"


@dp.view(
    name="exploded_city_entity",
    comment="Entry point: parsing raw JSON from Bronze into Silver structured format"
)
def exploded_city_entity():
    return (
        dp.read_stream(CITY_BRONZE_SOURCE)
        .select(
                *[col(f) for f in CITY_META_FIELDS],
            from_json(col("raw_json"), schemas.city_resource_schema).alias("data_json")
        )
        .select(
            *[col(f) for f in CITY_META_FIELDS],
            explode(col("data_json.CityResource.Cities.City")).alias(entity_alias)
        )
        .select(
            "*",
            upper(trim(col(f"{entity_alias}.{code_field}"))).alias(code_alias)
        )
    )


@dp.view(
    name="array_city_names_flat_raw"
)
def array_city_names_flat_raw():
    return (
        dp.read_stream("exploded_city_entity")
        .select(
            *[col(f) for f in CITY_META_FIELDS],
            col(code_alias),
            explode(col(f"{entity_alias}.Names.Name")).alias("n")
        )
        .select(
            *[col(f) for f in CITY_META_FIELDS],
            col(code_alias),
            lower(trim(col("n.`@LanguageCode`"))).alias("language_code"),
            col("n.$").alias(name_alias)
        )
    )


@dp.table(name="valid_city_names_flat")
@dp.expect_all_or_drop(CITY_RULES["ref_city_names_flat"])
def array_city_names_flat_df():
    return dp.read_stream("array_city_names_flat_raw")


@dp.table(name="valid_city_airport_map")
@dp.expect_all_or_drop(CITY_RULES["ref_city_airport_map"])
def array_city_airport_map_df():
    return (
        dp.read_stream("exploded_city_entity")
        .select(
            *[col(f) for f in CITY_META_FIELDS],
            col(code_alias),
            explode(col(f"{entity_alias}.Airports.AirportCode")).alias("airport_code")
        )
    )


@dp.table(name="silver_audit.err_city_invalid_json")
def invalid_json():
    return (
        dp.read_stream(CITY_BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.city_resource_schema))
        .filter(col("parsed").isNull())
        .select(*CITY_META_FIELDS, "raw_json")
    )


@dp.table(name="silver_audit.err_city_quarantine")
def city_quarantine():
    df = dp.read_stream("array_city_names_flat_raw")
    rules = CITY_RULES["ref_city_names_flat"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    return df.filter(f"NOT ({combined_condition})")


@dp.view(
    name="city_dim_df"
)
@dp.expect_all_or_drop(CITY_RULES["ref_dim_city"])
def city_dim_df():
    extra_fields={
            "CountryCode": "country_code",
            "UtcOffset": "utc_offset",
            "TimeZoneId": "time_zone_id",
        }
    select_exprs = [
        *[col(f) for f in CITY_META_FIELDS],
        col(code_alias),
    ]

    if extra_fields:
        for source_field, target_alias in extra_fields.items():
            select_exprs.append(
                col(f"{entity_alias}.{source_field}").alias(target_alias)
            )

    df = dp.read_stream("exploded_city_entity").select(*select_exprs)
    return df


dp.create_streaming_table("silver.ref_dim_city")
dp.create_auto_cdc_flow(
    target = "silver.ref_dim_city",
    source = "city_dim_df",
    keys = ["city_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 1
)


dp.create_streaming_table("silver.ref_city_names_flat")
dp.create_auto_cdc_flow(
    target = "silver.ref_city_names_flat",
    source = "valid_city_names_flat",
    keys = ["city_code", "language_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 2,
    track_history_column_list = ["city_name"]
)

dp.create_streaming_table("silver.ref_city_airport_map")
dp.create_auto_cdc_flow(
    target = "silver.ref_city_airport_map",
    source = "valid_city_airport_map",
    keys = ["city_code", "airport_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 2,
)
