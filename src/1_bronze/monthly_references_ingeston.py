import sys, os, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args,_ = parser.parse_known_args()
if args.root_path and args.root_path not in sys.path:
    sys.path.insert(0, args.root_path)
    
from src.app.init_app import init_app
from src.app.logger import get_logger
from src.config.endpoints import EndpointKeys
from src.services.ingestion_service import get_and_save_all_pages


def ingest_reference_entity_monthly(endpoint):
    logger = get_logger(__name__)

    logger.info(f"Start monthly reference ingest for {endpoint.value}")
    get_and_save_all_pages(
        endpoint=endpoint,
        limit=100,
        time_period="monthly",
    )

    logger.info(f"Finish monthly reference ingest for {endpoint.value}")


def ingest_references_monthly():
    logger = get_logger("jobs.ingestion_monthly")
    logger.info("Start monthly references ingestion job")
    init_app()
    reference_endpoints = [
        # EndpointKeys.AIRPORTS,
        # EndpointKeys.CITIES,
        EndpointKeys.COUNTRIES,
        # EndpointKeys.AIRLINES,
        # EndpointKeys.AIRCRAFT,
    ]

    for endpoint in reference_endpoints:
        try:
            ingest_reference_entity_monthly(endpoint)
        except Exception as e:
            logger.exception(f"Monthly ingest failed for {endpoint.value}: {e}")
            logger.warning(f"Sent message to Kafka")

    logger.info("Finish monthly references ingestion job")

def main():
    init_app()
    ingest_references_monthly()

if __name__ == "__main__":
    main()