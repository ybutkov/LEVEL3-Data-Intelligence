from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower, expr, concat_ws, coalesce, lit, try_to_timestamp, substring, replace
import src.services.parsing_schemas as schemas
from src.services.scd.utils.rules import OPERATIONAL_RULES

FLIGHT_STATUS_BRONZE_SOURCE = "lufthansa_level.bronze.flightstatus_by_route_raw"
FLIGHT_STATUS_META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias = "flight"

LOCAL_TS_FORMAT = "yyyy-MM-dd'T'HH:mm"
UTC_TS_FORMAT = "yyyy-MM-dd'T'HH:mmX"

def generate_flight_key(df, entity_alias="flight"):
    return df.withColumn(
        "flight_key",
        concat_ws("-", 
            upper(trim(col(f"{entity_alias}.MarketingCarrier.AirlineID"))),
            upper(trim(col(f"{entity_alias}.MarketingCarrier.FlightNumber"))),
            upper(trim(col(f"{entity_alias}.OperatingCarrier.AirlineID"))),
            upper(trim(col(f"{entity_alias}.OperatingCarrier.FlightNumber"))),
            upper(trim(col(f"{entity_alias}.Departure.AirportCode"))),
            upper(trim(col(f"{entity_alias}.Arrival.AirportCode"))),
            coalesce(
                replace(substring(col(f"{entity_alias}.Departure.ScheduledTimeLocal.DateTime"), 1, 10), lit("-"), lit("")),
                lit("")
            ),
            coalesce(
                replace(substring(col(f"{entity_alias}.Departure.ScheduledTimeLocal.DateTime"), 12, 5), lit(":"), lit("")),
                lit("")
            )
        )
    )


@dp.table(name="silver_audit.err_flight_status_invalid_json")
def flight_status_invalid_json():
    return (
        dp.read_stream(FLIGHT_STATUS_BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.flight_status_resource_schema))
        .filter(col("parsed").isNull())
        .select(
            *FLIGHT_STATUS_META_FIELDS,
            col("raw_json")
        )
    )


@dp.view(name="exploded_flight_status_entity")
def exploded_flight_status_entity():
    return (
        dp.read_stream(FLIGHT_STATUS_BRONZE_SOURCE)
        .select(
            *[col(f) for f in FLIGHT_STATUS_META_FIELDS],
            from_json(col("raw_json"), schemas.flight_status_resource_schema).alias("data_json")
        )
        .filter(col("data_json").isNotNull())
        .select(
            *[col(f) for f in FLIGHT_STATUS_META_FIELDS],
            explode(col("data_json.FlightStatusResource.Flights.Flight")).alias(entity_alias)
        )
    )


@dp.view
def flight_status_rules_checked():
    df = dp.read_stream("exploded_flight_status_entity")
    
    rules = OPERATIONAL_RULES.get("fact_flight_status", {})
    if rules:
        combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
        df = df.filter(combined_condition)
    
    df = generate_flight_key(df, entity_alias)
    
    quarantine_rules = OPERATIONAL_RULES.get("fact_flight_identity", {})
    if quarantine_rules:
        combined_quarantine = " AND ".join([f"({cond})" for cond in quarantine_rules.values()])
        is_quarantined = f"NOT({combined_quarantine})"
    else:
        is_quarantined = "false"
    
    return df.withColumn("is_quarantined", expr(is_quarantined))


@dp.view
def fact_flight_identity():
    return (
        dp.read_stream("flight_status_rules_checked")
        .filter("is_quarantined=false")
        .select(
            *FLIGHT_STATUS_META_FIELDS,
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
        .dropDuplicates(["flight_key"])
    )


@dp.table(name="silver_audit.err_flight_identity_quarantine")
def flight_identity_quarantine():
    return (
        dp.read_stream("flight_status_rules_checked")
        .filter("is_quarantined=true")
        .select(
            *FLIGHT_STATUS_META_FIELDS,
            col("flight_key"),
            upper(trim(col(f"{entity_alias}.MarketingCarrier.AirlineID"))).alias("marketing_airline_id"),
            upper(trim(col(f"{entity_alias}.MarketingCarrier.FlightNumber"))).alias("marketing_flight_number"),
            upper(trim(col(f"{entity_alias}.Departure.AirportCode"))).alias("departure_airport_code"),
            upper(trim(col(f"{entity_alias}.Arrival.AirportCode"))).alias("arrival_airport_code"),
            try_to_timestamp(col(f"{entity_alias}.Departure.ScheduledTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("scheduled_departure_utc"),
            expr("to_json(flight)").alias("raw_flight_json"),
        )
        .dropDuplicates(["flight_key"])
    )


@dp.view
def fact_flight_status():
    return (
        dp.read_stream("flight_status_rules_checked")
        .filter("is_quarantined=false")
        .select(
            *FLIGHT_STATUS_META_FIELDS,
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


@dp.table(name="silver_audit.err_flight_status_quarantine")
def flight_status_quarantine():
    return (
        dp.read_stream("flight_status_rules_checked")
        .filter("is_quarantined=true")
        .select(
            *FLIGHT_STATUS_META_FIELDS,
            col("flight_key"),
            upper(trim(col(f"{entity_alias}.Departure.AirportCode"))).alias("departure_airport_code"),
            upper(trim(col(f"{entity_alias}.Arrival.AirportCode"))).alias("arrival_airport_code"),
            try_to_timestamp(col(f"{entity_alias}.Departure.ScheduledTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("scheduled_departure_utc"),
            try_to_timestamp(col(f"{entity_alias}.Departure.ActualTimeUTC.DateTime"), lit(UTC_TS_FORMAT)).alias("actual_departure_utc"),
            col(f"{entity_alias}.FlightStatus.Code").alias("flight_status_code"),
            expr("to_json(flight)").alias("raw_flight_json"),
        )
    )


@dp.table(name="silver_audit.debug_flight_status_missing_datetime")
def debug_flight_status_missing_datetime():
    return (
        dp.read_stream("flight_status_rules_checked")
        .filter("is_quarantined=false")
        .filter(col(f"{entity_alias}.Departure.ScheduledTimeUTC.DateTime").isNull())
        .select(
            *FLIGHT_STATUS_META_FIELDS,
            col("flight_key"),
            expr("to_json(flight)").alias("raw_flight_json"),
        )
    )


dp.create_streaming_table("silver.fact_flight_identity")
dp.create_auto_cdc_flow(
    target="silver.fact_flight_identity",
    source="fact_flight_identity",
    keys=["flight_key"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=1,
)

dp.create_streaming_table("silver.fact_flight_status")
dp.create_auto_cdc_flow(
    target="silver.fact_flight_status",
    source="fact_flight_status",
    keys=["flight_key"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=1,
)
