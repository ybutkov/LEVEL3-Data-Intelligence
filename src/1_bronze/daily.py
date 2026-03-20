from src.app.init_app import init_app
from src.app.logger import get_logger
from src.config.endpoints import EndpointKeys
from src.services.ingestion_service import get_split_and_save_request
from src.services.storage_service import load_json_to_bronze_autoloader


def ingest_operational_entity_daily(endpoint, path_params=None, query_params=None):
    logger = get_logger(__name__)

    logger.info(f"Start daily operational ingest for {endpoint.value}")
    get_split_and_save_request(
        endpoint=endpoint,
        path_params=path_params,
        query_params=query_params,
        time_period="daily",
    )

    # load_json_to_bronze_autoloader(
    #     endpoint=endpoint,
    #     time_period="daily",
    # )
    logger.info(f"Finish daily operational ingest for {endpoint.value}")


def ingest_operational_daily():
    logger = get_logger("jobs.daily_ingestion")
    logger.info("Start daily operational ingestion job")

    # TODO: Put logic for ingesting all endpoints here
    for _ in range(2):
        path_params = {
            "origin": "FRA",
            "destination": "ZRH",
            "fromDateTime": "2026-03-20",
        }
    
        ingest_operational_entity_daily(
            endpoint=EndpointKeys.FLIGHTSTATUS_BY_ROUTE,
            path_params=path_params,
            query_params={},
        )

        query_params = {
            "airlines": "LH",
            "startDate": "10MAR26",
            "endDate": "15MAR26",
            "daysOfOperation": "1234567",
            "timeMode": "UTC"
        }
        ingest_operational_entity_daily(
            EndpointKeys.FLIGHT_SCHEDULES, 
            query_params=query_params
        )

    logger.info("Finish daily operational ingestion job")

def main():
    init_app()
    ingest_operational_daily()

if __name__ == "__main__":
    main()
