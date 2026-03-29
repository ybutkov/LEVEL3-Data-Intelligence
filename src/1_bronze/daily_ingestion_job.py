import sys, os, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args,_ = parser.parse_known_args()
if args.root_path and args.root_path not in sys.path:
    sys.path.insert(0, args.root_path)


import uuid
from datetime import date
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from src.services.ingestion_service import get_and_save_all_pages
from src.config.endpoints import EndpointKeys
from src.app.init_app import init_app
from src.app.logger import get_logger
from src.config.config_properties import get_ConfigProperties

spark = SparkSession.builder.getOrCreate()

CATALOG = "lufthansa_level"
TARGET_COUNTRIES = ['DE', 'AT', 'CH']
API_BASE_URL = "https://api.lufthansa.com/v1/operations/flightstatus"

TABLES = {
    "departures": f"{CATALOG}.bronze.flightstatus_departures_raw",
    "arrivals": f"{CATALOG}.bronze.flightstatus_arrivals_raw"
}


logger = get_logger(__name__)


def fetch_and_save():
    logger.info("Start daily operational ingestion job")
    cfg = get_ConfigProperties()
    now = datetime.now()
    from_date_time = now.strftime("%Y-%m-%dT%H:%M")

    silver_dim_airport = f"{cfg.storage.catalog}.{cfg.storage.silver_schema}.ref_dim_airport"

    country_codes_sql = ",".join([f"'{code}'" for code in TARGET_COUNTRIES])
    
    try:
        airports = (
            spark
                .table(silver_dim_airport)
                .filter(f"country_code IN ({country_codes_sql})")
                .select("airport_code")
                .distinct()
                .collect()
        )
    except Exception as e:
        logger.warning(f"Cannot read ref_dim_airport (may not exist yet): {e}. ")
        return

    # path_params = {
    #         "origin": "FRA",
    #         "destination": "ZRH",
    #         "fromDateTime": from_date_time,
    #     }
    # get_and_save_all_pages(
    #             endpoint=EndpointKeys.FLIGHTSTATUS_BY_ROUTE,
    #             path_params=path_params,
    #             query_params={},
    #             limit=50,
    #             time_period="daily",
    #         )

    for row in airports:
        
        try:
            # operations/flightstatus/departures/FRA/2026-03-21T08:00
            # operations/flightstatus/arrivals/FRA/2026-03-21T08:00
            # limit=50 !!!!!!
            path_params = {
                "airportCode": f"{row.airport_code}",
                "fromDateTime": f"{from_date_time}",
            }
            logger.info(f"Fetch flights by departure for : {row.airport_code}. Date: {from_date_time}")
            get_and_save_all_pages(
                EndpointKeys.FLIGHTSTATUS_BY_DEPARTURE, 
                path_params=path_params,
                limit=50,
                time_period="daily"
            )
            logger.info(f"Fetch flights by arrival for : {row.airport_code}. Date: {from_date_time}")
            get_and_save_all_pages(
                EndpointKeys.FLIGHTSTATUS_BY_ARRIVAL, 
                path_params=path_params,
                limit=50,
                time_period="daily"
            )
                
        except Exception as e:
            # Error already logged in ingestion_service. Skip this airport
            logger.debug(f"Failed to fetch {row.airport_code}: {str(e)}")
            pass
    logger.info("Finish daily operational ingestion job")

if __name__ == "__main__":
    init_app()
    fetch_and_save()


