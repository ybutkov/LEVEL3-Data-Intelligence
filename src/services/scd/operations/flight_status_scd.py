import sys
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

# Add workspace root to path for imports
try:
    root_path = spark.conf.get("root_path")
    if root_path:
        sys.path.insert(0, root_path)
except:
    pass

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower, expr, concat_ws, coalesce, lit, try_to_timestamp, substring, replace, when
import src.services.parsing_schemas as schemas
from src.services.scd.utils.rules import OPERATIONAL_RULES
from src.util.tables_utils import build_full_table_name

# Configuration
CATALOG = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema")
SILVER_AUDIT_SCHEMA = spark.conf.get("silver_audit_schema")

SOURCES = {
    "by_route": build_full_table_name(CATALOG, BRONZE_SCHEMA, "flightstatus_by_route_raw"),
    "by_departure": build_full_table_name(CATALOG, BRONZE_SCHEMA, "flightstatus_by_departure_raw"),
    "by_arrival": build_full_table_name(CATALOG, BRONZE_SCHEMA, "flightstatus_by_arrival_raw")
}



META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias = "flight"

LOCAL_TS_FORMAT = "yyyy-MM-dd'T'HH:mm"
UTC_TS_FORMAT = "yyyy-MM-dd'T'HH:mmX"
# LOCAL_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
# UTC_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"


def generate_flight_key(col_ref):
    """
    Generate unique flight key from marketing carrier, airline, and airport codes.
    
    Concatenates airline IDs, flight numbers, and airport codes to create
    a composite key for identifying unique flights across different views.
    
    Args:
        col_ref: Column reference with nested Flight structure
        
    Returns:
        Column: Concatenated flight key with format "AIRLINE-FLIGHTNUM-AIRLINE-FLIGHTNUM-FROM-TO"
    """
    return concat_ws("-", 
        upper(trim(col_ref.MarketingCarrier.AirlineID)),
        upper(trim(col_ref.MarketingCarrier.FlightNumber)),
        upper(trim(col_ref.OperatingCarrier.AirlineID)),
        upper(trim(col_ref.OperatingCarrier.FlightNumber)),
        upper(trim(col_ref.Departure.AirportCode)),
        upper(trim(col_ref.Arrival.AirportCode)),
    )


def build_flight_flow(source_key, source_table):
    """
    Build streaming flight data pipeline from bronze to silver.
    
    Creates exploded and validated views for flight status data, applying
    operational rules validation and generating flight keys.
    
    Args:
        source_key (str): Identifier for flight status source (by_route, by_departure, etc.)
        source_table (str): Bronze table path to read streaming data from
    """
    
    # @dp.view(name=f"exploded_{source_key}")
    @dp.view(name=f"exploded_{source_key}")
    def exploded_view():
        return (
            dp.read_stream(source_table)
            .select(
                col("source_file"),
                col("bronze_ingested_at"),
                col("ingest_run_id"),
                lit(source_key).alias("source_endpoint"),
                from_json(col("raw_json"), schemas.flight_status_resource_schema).alias("data_json")
            )
            .filter(col("data_json").isNotNull())
            .select(
                col("source_file"),
                col("bronze_ingested_at"),
                col("ingest_run_id"),
                col("source_endpoint"),
                explode(col("data_json.FlightStatusResource.Flights.Flight")).alias(entity_alias)
            )
        )
    
    # @dp.view(name=f"validated_{source_key}")
    # def validated_view(key=source_key, path=source_table):
    #     df = (
    #         dp.read_stream(path)
    #        .select(*META_FIELDS, from_json(col("raw_json"), schemas.flight_status_resource_schema).alias("data"))
    #        .filter(col("data").isNotNull())
    #        .select(*META_FIELDS, explode(col("data.FlightStatusResource.Flights.Flight")).alias(entity_alias))
    #     )
        
    #     df = generate_flight_key(df, entity_alias)
    #     rules = OPERATIONAL_RULES["fact_flight_status"]
    #     combined_cond = " AND ".join([f"({cond})" for cond in rules.values()])
        
    #     return df.withColumn("is_quarantined", expr(f"CASE WHEN ({combined_cond}) IS TRUE THEN false ELSE true END"))

    @dp.view(name=f"validated_{source_key}")
    def validated_view():
        df = dp.read_stream(f"exploded_{source_key}")
        
        rules = OPERATIONAL_RULES.get("fact_flight_status", {})
        if rules:
            combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
            df = df.filter(combined_condition)
        
        df = df.withColumn("flight_key", generate_flight_key(col(entity_alias)))
        
        quarantine_rules = OPERATIONAL_RULES.get("fact_flight_identity", {})
        if quarantine_rules:
            # TODO: NULL problems !!!
            combined_quarantine = " AND ".join([f"({cond})" for cond in quarantine_rules.values()])
            is_quarantined = f"NOT({combined_quarantine})"
        else:
            is_quarantined = "false"
        
        return df.withColumn("is_quarantined", expr(is_quarantined))
    
    # @dp.table(name=f"silver_audit.err_flight_status_{source_key}_quarantine")
    @dp.append_flow(
        target = build_full_table_name(CATALOG, SILVER_AUDIT_SCHEMA, "err_flight_status_quarantine"),
        name = f"flow_quarantine_{source_key}"
    )
    def quarantine_table():
        return (
            dp.read_stream(f"validated_{source_key}")
            .filter("is_quarantined=true")
            .select(
                *META_FIELDS,
                col("source_endpoint"),
                col("flight_key"),
                upper(trim(col(f"{entity_alias}.Departure.AirportCode"))).alias("departure_airport_code"),
                upper(trim(col(f"{entity_alias}.Arrival.AirportCode"))).alias("arrival_airport_code"),
                try_to_timestamp(col(f"{entity_alias}.Departure.ScheduledTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("scheduled_departure_utc"),
                try_to_timestamp(col(f"{entity_alias}.Departure.ActualTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("actual_departure_utc"),
                col(f"{entity_alias}.FlightStatus.Code").alias("flight_status_code"),
                expr("to_json(flight)").alias("raw_flight_json"),
            )
        )
    
    # @dp.table(name=f"silver_audit.err_flight_status_invalid_json_{source_key}")
    @dp.append_flow(
        target = build_full_table_name(CATALOG, SILVER_AUDIT_SCHEMA, "err_flight_status_invalid_json"),
        name = f"flow_invalid_json_{source_key}"
    )
    def invalid_json_table():
        return (
            dp.read_stream(source_table)
            .withColumn("parsed", from_json(col("raw_json"), schemas.flight_status_resource_schema))
            .filter(col("parsed").isNull())
            .select(
                col("source_file"),
                col("bronze_ingested_at"),
                col("ingest_run_id"),
                lit(source_key).alias("source_endpoint"),
                col("raw_json")
            )
        )
    
    @dp.view(name=f"clean_flight_identity_{source_key}")
    def clean_flight_identity_view():
        return (
            dp.read_stream(f"validated_{source_key}")
            .filter("is_quarantined=false")
            .select(
                *META_FIELDS,
                col("source_endpoint"),
                col("flight_key"),
                upper(trim(col(f"{entity_alias}.MarketingCarrier.AirlineID"))).alias("marketing_airline_id"),
                upper(trim(col(f"{entity_alias}.MarketingCarrier.FlightNumber"))).alias("marketing_flight_number"),
                upper(trim(col(f"{entity_alias}.OperatingCarrier.AirlineID"))).alias("operating_airline_id"),
                upper(trim(col(f"{entity_alias}.OperatingCarrier.FlightNumber"))).alias("operating_flight_number"),
                upper(trim(col(f"{entity_alias}.Departure.AirportCode"))).alias("departure_airport_code"),
                upper(trim(col(f"{entity_alias}.Arrival.AirportCode"))).alias("arrival_airport_code"),
                try_to_timestamp(col(f"{entity_alias}.Departure.ScheduledTimeLocal.DateTime"), lit(LOCAL_TS_FORMAT)).alias("scheduled_departure_local"),
                try_to_timestamp(col(f"{entity_alias}.Departure.ScheduledTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("scheduled_departure_utc"),
                try_to_timestamp(col(f"{entity_alias}.Arrival.ScheduledTimeLocal.DateTime"), lit(LOCAL_TS_FORMAT)).alias("scheduled_arrival_local"),
                try_to_timestamp(col(f"{entity_alias}.Arrival.ScheduledTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("scheduled_arrival_utc"),
                upper(trim(col(f"{entity_alias}.ServiceType"))).alias("service_type"),
            )
            # .dropDuplicates(["flight_key"])
        )
    
    @dp.view(name=f"clean_flight_status_{source_key}")
    def clean_flight_status_view():
        return (
            dp.read_stream(f"validated_{source_key}")
            .filter("is_quarantined=false")
            .select(
                *META_FIELDS,
                col("source_endpoint"),
                col("flight_key"),
                try_to_timestamp(col(f"{entity_alias}.Departure.ScheduledTimeLocal.DateTime"), lit(LOCAL_TS_FORMAT)).alias("scheduled_departure_local"),
                try_to_timestamp(col(f"{entity_alias}.Departure.ScheduledTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("scheduled_departure_utc"),
                try_to_timestamp(col(f"{entity_alias}.Departure.ActualTimeLocal.DateTime"), lit(LOCAL_TS_FORMAT)).alias("actual_departure_local"),
                try_to_timestamp(col(f"{entity_alias}.Departure.ActualTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("actual_departure_utc"),
                try_to_timestamp(col(f"{entity_alias}.Departure.EstimatedTimeLocal.DateTime"), lit(LOCAL_TS_FORMAT)).alias("estimated_departure_local"),
                try_to_timestamp(col(f"{entity_alias}.Departure.EstimatedTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("estimated_departure_utc"),
                upper(trim(col(f"{entity_alias}.Departure.TimeStatus.Code"))).alias("departure_time_status_code"),
                trim(col(f"{entity_alias}.Departure.Terminal.Name")).alias("departure_terminal_name"),
                upper(trim(col(f"{entity_alias}.Departure.Terminal.Gate"))).alias("departure_gate"),
                try_to_timestamp(col(f"{entity_alias}.Arrival.ScheduledTimeLocal.DateTime"), lit(LOCAL_TS_FORMAT)).alias("scheduled_arrival_local"),
                try_to_timestamp(col(f"{entity_alias}.Arrival.ScheduledTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("scheduled_arrival_utc"),
                try_to_timestamp(col(f"{entity_alias}.Arrival.ActualTimeLocal.DateTime"), lit(LOCAL_TS_FORMAT)).alias("actual_arrival_local"),
                try_to_timestamp(col(f"{entity_alias}.Arrival.ActualTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("actual_arrival_utc"),
                try_to_timestamp(col(f"{entity_alias}.Arrival.EstimatedTimeLocal.DateTime"), lit(LOCAL_TS_FORMAT)).alias("estimated_arrival_local"),
                try_to_timestamp(col(f"{entity_alias}.Arrival.EstimatedTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("estimated_arrival_utc"),
                upper(trim(col(f"{entity_alias}.Arrival.TimeStatus.Code"))).alias("arrival_time_status_code"),
                trim(col(f"{entity_alias}.Arrival.Terminal.Name")).alias("arrival_terminal_name"),
                upper(trim(col(f"{entity_alias}.Arrival.Terminal.Gate"))).alias("arrival_gate"),
                upper(trim(col(f"{entity_alias}.Equipment.AircraftCode"))).alias("aircraft_code"),
                upper(trim(col(f"{entity_alias}.Equipment.AircraftRegistration"))).alias("aircraft_registration"),
                upper(trim(col(f"{entity_alias}.FlightStatus.Code"))).alias("flight_status_code"),
            )
        )


dp.create_streaming_table(build_full_table_name(CATALOG, SILVER_AUDIT_SCHEMA, "err_flight_status_invalid_json"))
dp.create_streaming_table(build_full_table_name(CATALOG, SILVER_AUDIT_SCHEMA, "err_flight_status_quarantine"))
dp.create_streaming_table(build_full_table_name(CATALOG, SILVER_SCHEMA, "fact_flight_status"))
dp.create_streaming_table(build_full_table_name(CATALOG, SILVER_SCHEMA, "fact_flight_identity"))

for source_key, source_table in SOURCES.items():

    build_flight_flow(source_key, source_table)

    dp.create_auto_cdc_flow(
        name=f"cdc_flight_identity_{source_key}",
        target=build_full_table_name(CATALOG, SILVER_SCHEMA, "fact_flight_identity"),
        source=f"clean_flight_identity_{source_key}",
        keys=["flight_key"],
        sequence_by=col("bronze_ingested_at"),
        ignore_null_updates=True,
        stored_as_scd_type=1,
    )

    dp.create_auto_cdc_flow(
        name=f"cdc_flight_status_{source_key}",
        target=build_full_table_name(CATALOG, SILVER_SCHEMA, "fact_flight_status"),
        source=f"clean_flight_status_{source_key}",
        keys=["flight_key"],
        sequence_by=col("bronze_ingested_at"),
        ignore_null_updates=True,
        stored_as_scd_type=1,
    )
