from app.init_app import init_app
from app.logger import get_logger
from config.endpoints import EndpointKeys
from services.ingestion_service import get_split_and_save_request
from services.storage_service import load_json_to_bronze_autoloader


def ingest_operational_entity_daily(endpoint, path_params=None, query_params=None):
    logger = get_logger(__name__)

    logger.info(f"Start daily operational ingest for {endpoint.value}")
    get_split_and_save_request(
        endpoint=endpoint,
        path_params=path_params,
        query_params=query_params,
        time_period="daily",
    )

    load_json_to_bronze_autoloader(
        endpoint=endpoint,
        time_period="daily",
    )
    logger.info(f"Finish daily operational ingest for {endpoint.value}")


def ingest_operational_daily():
    logger = get_logger("jobs.daily_ingestion")
    logger.info("Start daily operational ingestion job")

    # TODO: Put logic for ingesting all endpoints here
    path_params = {
        "departure_airport_code": "FRA",
        "arrival_airport_code": "JFK",
        "date": "2026-03-18",
    }
    
    ingest_operational_entity_daily(
        endpoint=EndpointKeys.FLIGHTSTATUS_BY_ROUTE,
        path_params=path_params,
        query_params={},
    )

    logger.info("Finish daily operational ingestion job")
