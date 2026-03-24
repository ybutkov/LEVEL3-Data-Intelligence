from pyspark.sql.functions import col, explode, coalesce

from src.services.parser.parser_orchestrator import run_parser
from src.config.endpoints import EndpointKeys
import src.services.parsing_schemas as schemas
from src.services.parser.operations.operations_rules import OPERATIONAL_RULES
from src.services.parser.utils.normalize_utils import OPERATIONAL_NORMALIZATION_MAP
from src.services.parser.operations.operations_transforms import OPERATIONAL_TRANSFORMATION_MAP
from src.app.logger import get_logger


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

            col("flight.MarketingCarrier.AirlineID").alias("airline_id"),
            col("flight.MarketingCarrier.FlightNumber").alias("flight_number"),

            col("flight.OperatingCarrier.AirlineID").alias("operating_airline_id"),
            col("flight.OperatingCarrier.FlightNumber").alias("operating_flight_number"),

            col("flight.Equipment.AircraftCode").alias("aircraft_code"),

            col("flight.Departure.AirportCode").alias("dep_airport"),
            col("flight.Arrival.AirportCode").alias("arr_airport"),

            col("flight.Departure.ScheduledTimeLocal.DateTime").alias("sched_dep_local"),
            col("flight.Departure.ScheduledTimeUTC.DateTime").alias("sched_dep_utc"),

            coalesce(
                col("flight.Departure.ActualTimeLocal.DateTime"),
                col("flight.Departure.EstimatedTimeLocal.DateTime"),
            ).alias("best_dep_local"),

            coalesce(
                col("flight.Departure.ActualTimeUTC.DateTime"),
                col("flight.Departure.EstimatedTimeUTC.DateTime"),
            ).alias("best_dep_utc"),

            col("flight.Arrival.ScheduledTimeLocal.DateTime").alias("sched_arr_local"),
            col("flight.Arrival.ScheduledTimeUTC.DateTime").alias("sched_arr_utc"),

            coalesce(
                col("flight.Arrival.ActualTimeLocal.DateTime"),
                col("flight.Arrival.EstimatedTimeLocal.DateTime"),
            ).alias("best_arr_local"),

            coalesce(
                col("flight.Arrival.ActualTimeUTC.DateTime"),
                col("flight.Arrival.EstimatedTimeUTC.DateTime"),
            ).alias("best_arr_utc"),

            col("flight.Departure.TimeStatus.Code").alias("dep_time_status_code"),
            col("flight.Arrival.TimeStatus.Code").alias("arr_time_status_code"),
            col("flight.FlightStatus.Code").alias("flight_status_code"),
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
