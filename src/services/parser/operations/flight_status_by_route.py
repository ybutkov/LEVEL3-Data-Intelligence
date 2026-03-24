from pyspark.sql.functions import col, explode

from src.services.parser.parser_orchestrator import run_parser
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.app.logger import get_logger

from src.services.parser.utils.normalize_utils import OPERATIONAL_NORMALIZATION_MAP
from src.services.parser.operations.operations_transforms import OPERATIONAL_TRANSFORMATION_MAP
from src.services.parser.operations.operations_rules import OPERATIONAL_RULES


logger = get_logger(__name__)

def build_flight_status_outputs(valid_df):
    logger.info("Start build_flight_status_outputs")

    exploded_df = (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.FlightStatusResource.Flights.Flight")).alias("flight")
        )
    )

    logger.info("Start build op_fact_flight_status")
    op_fact_flight_status_df = (
        exploded_df
        .select(
            col("source_file"),
            col("bronze_ingested_at"),

            col("flight.MarketingCarrier.AirlineID").alias("marketing_airline_id"),
            col("flight.MarketingCarrier.FlightNumber").alias("marketing_flight_number"),

            col("flight.OperatingCarrier.AirlineID").alias("operating_airline_id"),
            col("flight.OperatingCarrier.FlightNumber").alias("operating_flight_number"),

            col("flight.Departure.AirportCode").alias("departure_airport_code"),
            col("flight.Arrival.AirportCode").alias("arrival_airport_code"),

            col("flight.Departure.ScheduledTimeLocal.DateTime").alias("scheduled_departure_local"),
            col("flight.Departure.ScheduledTimeUTC.DateTime").alias("scheduled_departure_utc"),
            col("flight.Departure.ActualTimeLocal.DateTime").alias("actual_departure_local"),
            col("flight.Departure.ActualTimeUTC.DateTime").alias("actual_departure_utc"),
            col("flight.Departure.EstimatedTimeLocal.DateTime").alias("estimated_departure_local"),
            col("flight.Departure.EstimatedTimeUTC.DateTime").alias("estimated_departure_utc"),

            col("flight.Arrival.ScheduledTimeLocal.DateTime").alias("scheduled_arrival_local"),
            col("flight.Arrival.ScheduledTimeUTC.DateTime").alias("scheduled_arrival_utc"),
            col("flight.Arrival.ActualTimeLocal.DateTime").alias("actual_arrival_local"),
            col("flight.Arrival.ActualTimeUTC.DateTime").alias("actual_arrival_utc"),
            col("flight.Arrival.EstimatedTimeLocal.DateTime").alias("estimated_arrival_local"),
            col("flight.Arrival.EstimatedTimeUTC.DateTime").alias("estimated_arrival_utc"),

            col("flight.Departure.TimeStatus.Code").alias("departure_time_status_code"),
            col("flight.Departure.TimeStatus.Definition").alias("departure_time_status_definition"),

            col("flight.Arrival.TimeStatus.Code").alias("arrival_time_status_code"),
            col("flight.Arrival.TimeStatus.Definition").alias("arrival_time_status_definition"),

            col("flight.Departure.Terminal.Name").alias("departure_terminal_name"),
            col("flight.Departure.Terminal.Gate").alias("departure_gate"),

            col("flight.Arrival.Terminal.Name").alias("arrival_terminal_name"),
            col("flight.Arrival.Terminal.Gate").alias("arrival_gate"),

            col("flight.Equipment.AircraftCode").alias("aircraft_code"),
            col("flight.Equipment.AircraftRegistration").alias("aircraft_registration"),

            col("flight.FlightStatus.Code").alias("flight_status_code"),
            col("flight.FlightStatus.Definition").alias("flight_status_definition"),

            col("flight.ServiceType").alias("service_type"),
        )
    )

    logger.info("Finish build_flight_status_outputs")

    return {
        "op_fact_flight_status": op_fact_flight_status_df,
    }


def run_flight_status_by_route(spark, cfg):
    run_parser(
        spark=spark,
        cfg=cfg,
        endpoint_key=EndpointKeys.FLIGHTSTATUS_BY_ROUTE,
        schema=schemas.flight_status_resource_schema,
        build_outputs_fn=build_flight_status_outputs,
        normalization_map=OPERATIONAL_NORMALIZATION_MAP,
        transformation_map=OPERATIONAL_TRANSFORMATION_MAP,
        rules_map=OPERATIONAL_RULES,
    )
