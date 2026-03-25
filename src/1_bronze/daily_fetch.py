import sys, os, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args,_ = parser.parse_known_args()
if args.root_path and args.root_path not in sys.path:
    sys.path.insert(0, args.root_path)

from datetime import date
from src.app.init_app import init_app
from src.app.logger import get_logger
from src.config.endpoints import EndpointKeys
from src.services.ingestion_service import get_and_save_all_pages


logger = get_logger(__name__)

ROUTE_CANDIDATES = [
    ("FRA", "ZRH"),
    ("FRA", "VIE"),
    ("FRA", "MUC"),
    ("FRA", "BER"),
    ("FRA", "HAM"),
    ("FRA", "AMS"),
    ("FRA", "CDG"),
    ("FRA", "LHR"),
    ("MUC", "ZRH"),
    ("MUC", "VIE"),
    ("MUC", "BER"),
    ("MUC", "HAM"),
    ("BER", "VIE"),
    ("BER", "ZRH"),
]

def ingest_operational_entity_daily(endpoint, limit=20, path_params=None, query_params=None, time_period=None):

    # logger.info(f"Start daily operational ingest for {endpoint.value}")
    get_and_save_all_pages(
        endpoint=endpoint,
        path_params=path_params,
        query_params=query_params,
        limit=limit,
        time_period=time_period,
    )
    # logger.info(f"Finish daily operational ingest for {endpoint.value}")


def ingest_operational_daily():
    logger.info("Start daily operational ingestion job")

    flight_date = date.today().isoformat()

    for origin, destination in ROUTE_CANDIDATES:
        path_params = {
            "origin": origin,
            "destination": destination,
            "fromDateTime": flight_date,
        }

        logger.info(f"Fetch route {origin}->{destination} for {flight_date}")

        try:
            ingest_operational_entity_daily(
                endpoint=EndpointKeys.FLIGHTSTATUS_BY_ROUTE,
                path_params=path_params,
                query_params={},
                limit=80,
                time_period="daily",
            )
        except Exception as e:
            logger.exception(f"Failed route {origin}->{destination}: {e}")

    logger.info("Finish daily operational ingestion job")


def main():
    init_app()
    ingest_operational_daily()

if __name__ == "__main__":
    main()
