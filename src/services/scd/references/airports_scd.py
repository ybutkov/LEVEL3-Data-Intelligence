from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower
import src.services.parsing_schemas as schemas

from src.services.scd.utils.rules import AIRPORT_RULES

AIRPORT_BRONZE_SOURCE = "lufthansa_level.bronze.airports_raw"
AIRPORT_META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias = "airport"
code_field = "AirportCode"
code_alias = "airport_code"

@dp.view(
    name="exploded_airport_entity",
    comment="Parsing Airport JSON with coordinates and city mapping"
)
def exploded_airport_entity():
    return (
        dp.read_stream(AIRPORT_BRONZE_SOURCE)
        .select(
            *[col(f) for f in AIRPORT_META_FIELDS],
            from_json(col("raw_json"), schemas.airport_resource_schema).alias("data_json")
        )
        .select(
            *[col(f) for f in AIRPORT_META_FIELDS],
            # Путь: AirportResource -> Airports -> Airport
            explode(col("data_json.AirportResource.Airports.Airport")).alias(entity_alias)
        )
        .select(
            "*",
            upper(trim(col(f"{entity_alias}.{code_field}"))).alias(code_alias),
            # col(f"{entity_alias}.CityCode").alias("city_code"),
            # col(f"{entity_alias}.CountryCode").alias("country_code"),
            # # Извлекаем гео-данные
            # col(f"{entity_alias}.Position.Coordinate.Latitude").alias("latitude"),
            # col(f"{entity_alias}.Position.Coordinate.Longitude").alias("longitude"),
            # col(f"{entity_alias}.TimeZoneId").alias("time_zone_id")
        )
    )

@dp.view(
    name="array_airport_names_flat_raw"
)
def array_airport_names_flat_raw():
    return (
        dp.read_stream("exploded_airport_entity")
        .select(
            *[col(f) for f in AIRPORT_META_FIELDS],
            col(code_alias),
            explode(col(f"{entity_alias}.Names.Name")).alias("n")
        )
        .select(
            *[col(f) for f in AIRPORT_META_FIELDS],
            col(code_alias),
            lower(trim(col("n.`@LanguageCode`"))).alias("language_code"),
            col("n.$").alias("airport_name")
        )
    )

@dp.table(name="valid_airport_names_flat")
@dp.expect_all_or_drop(AIRPORT_RULES["ref_airport_names_flat"])
def array_airport_names_flat_df():
    return dp.read_stream("array_airport_names_flat_raw")


@dp.table(name="silver_audit.err_airport_invalid_json")
def invalid_json():
    return (
        dp.read_stream(AIRPORT_BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.airport_resource_schema))
        .filter(col("parsed").isNull())
        .select(*AIRPORT_META_FIELDS, "raw_json")
    )


@dp.table(name="silver_audit.err_airport_quarantine")
def airport_quarantine():
    df = dp.read_stream("array_airport_names_flat_raw")
    rules = AIRPORT_RULES["ref_airport_names_flat"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    return df.filter(f"NOT ({combined_condition})")


@dp.view(
    name="airport_dim_df"
)
@dp.expect_all_or_drop(AIRPORT_RULES["ref_dim_airport"])
def airport_dim_df():
    extra_fields={
            "CityCode": "city_code",
            "CountryCode": "country_code",
            "Position.Coordinate.Latitude": "latitude",
            "Position.Coordinate.Longitude": "longitude",
            "UtcOffset": "utc_offset",
            "TimeZoneId": "time_zone_id",
        }
    select_exprs = [
        *[col(f) for f in AIRPORT_META_FIELDS],
        col(code_alias),
    ]

    if extra_fields:
        for source_field, target_alias in extra_fields.items():
            select_exprs.append(
                col(f"{entity_alias}.{source_field}").alias(target_alias)
            )

    df = dp.read_stream("exploded_airport_entity").select(*select_exprs)
    return df

dp.create_streaming_table("silver.ref_dim_airport")
dp.create_auto_cdc_flow(
    target = "silver.ref_dim_airport",
    source = "airport_dim_df",
    keys = ["airport_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 2,
    track_history_column_list = [
        "city_code", "country_code",
        "latitude", "longitude",
        "utc_offset", "time_zone_id"
    ]
)

dp.create_streaming_table("silver.ref_airport_names_flat")
dp.create_auto_cdc_flow(
    target = "silver.ref_airport_names_flat",
    source = "valid_airport_names_flat",
    keys = ["airport_code", "language_code"],
    sequence_by = col("bronze_ingested_at"),
    stored_as_scd_type = 2,
    track_history_column_list = ["airport_name"]
)
