from app.init_app import init_app
from app.logger import get_logger
from config.endpoints import EndpointKeys
from services.ingestion_service import get_and_save_all_pages
from services.storage_service import load_json_to_bronze_autoloader


def ingest_reference_entity_monthly(endpoint):
    logger = get_logger(__name__)

    logger.info(f"Start monthly reference ingest for {endpoint.value}")
    get_and_save_all_pages(
        endpoint=endpoint,
        limit=100,
        time_period="monthly",
    )

    load_json_to_bronze_autoloader(
        endpoint=endpoint,
        time_period="monthly",
    )
    logger.info(f"Finish monthly reference ingest for {endpoint.value}")


def ingest_references_monthly():
    logger = get_logger("jobs.ingestion_monthly")
    logger.info("Start monthly references ingestion job")
    init_app()
    reference_endpoints = [
        EndpointKeys.AIRPORTS,
        EndpointKeys.CITIES,
        EndpointKeys.COUNTRIES,
        EndpointKeys.AIRLINES,
        EndpointKeys.AIRCRAFT,
    ]

    for endpoint in reference_endpoints:
        try:
            ingest_reference_entity_monthly(endpoint)
        except Exception as e:
            logger.exception(f"Monthly ingest failed for {endpoint.value}: {e}")
            logger.warning(f"Sent message to Kafka")

    logger.info("Finish monthly references ingestion job")

