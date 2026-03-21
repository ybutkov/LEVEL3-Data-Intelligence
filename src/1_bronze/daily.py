import sys, os, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args,_ = parser.parse_known_args()
if args.root_path:
    sys.path.insert(0, args.root_path)

from src.app.init_app import init_app
from src.app.logger import get_logger
from src.config.endpoints import EndpointKeys
from src.services.ingestion_service import get_split_and_save_request
from src.services.ingestion_service import get_and_save_all_pages
from src.services.storage_service import load_json_to_bronze_autoloader


def ingest_operational_entity_daily(endpoint, limit=20, path_params=None, query_params=None, time_period=None):
    logger = get_logger(__name__)

    logger.info(f"Start daily operational ingest for {endpoint.value}")
    get_and_save_all_pages(
        endpoint=endpoint,
        path_params=path_params,
        query_params=query_params,
        limit=limit,
        time_period=time_period,
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
    for _ in range(1, 2):
        path_params = {
            "origin": "FRA",
            "destination": "ZRH",
            "fromDateTime": "2026-03-20",
        }
    
        # ingest_operational_entity_daily(
        #     endpoint=EndpointKeys.FLIGHTSTATUS_BY_ROUTE,
        #     path_params=path_params,
        #     query_params={},
        #     limit=100,
        #     time_period="daily"
        # )

        # query_params = {
        #     "airlines": "LH",
        #     "startDate": "10MAR26",
        #     "endDate": "15MAR26",
        #     "daysOfOperation": "1234567",
        #     "timeMode": "UTC"
        # }
        # ingest_operational_entity_daily(
        #     EndpointKeys.FLIGHT_SCHEDULES, 
        #     query_params=query_params,
        #     time_period="daily"
        # )

# **********************************
        # operations/schedules/FRA/JFK/2026-03-21
        # not paginable
        path_params = {
            "origin": "FRA",
            "destination": "JFK",
            "fromDateTime": "2026-03-21",
        }
        ingest_operational_entity_daily(
            EndpointKeys.FLIGHTSCHEDULES_BY_ROUTE, 
            path_params=path_params,
            time_period=None
        )
        
        # operations/flightstatus/departures/FRA/2026-03-21T08:00
        # limit=50 !!!!!!
        path_params = {
            "airportCode": "FRA",
            "fromDateTime": "2026-03-21T08:00",
        }
        ingest_operational_entity_daily(
            EndpointKeys.FLIGHTSTATUS_BY_DEPARTURE, 
            path_params=path_params,
            limit=50,
            time_period="daily"
        )

        # operations/flightstatus/arrivals/FRA/2026-03-21T08:00
        # limit=50 !!!!!!
        path_params = {
            "airportCode": "FRA",
            "fromDateTime": "2026-03-21T08:00",
        }
        ingest_operational_entity_daily(
            EndpointKeys.FLIGHTSTATUS_BY_ARRIVAL, 
            path_params=path_params,
            limit=50,
            time_period="daily"
        )

    logger.info("Finish daily operational ingestion job")

def main():
    init_app()
    ingest_operational_daily()

if __name__ == "__main__":
    main()
