from app.init_app import init_app
from services.ingestion_service import get_and_save_all_pages
from config.endpoints import EndpointKeys
from app.logger import get_logger

def monthly_bronze_lufthansa_ingestion():
    get_and_save_all_pages(EndpointKeys.COUNTRIES, limit=100, time_period="monthly")
    get_and_save_all_pages(EndpointKeys.CITIES, limit=100, time_period="monthly")
    get_and_save_all_pages(EndpointKeys.AIRPORTS, limit=100, time_period="monthly")
    get_and_save_all_pages(EndpointKeys.AIRLINES, limit=100, time_period="monthly")
    get_and_save_all_pages(EndpointKeys.AIRCRAFT, limit=100, time_period="monthly")

def main():
    init_app()
    logger = get_logger("jobs.monthly_bronze_lufthansa_ingestion")
    logger.info("Starting monthly bronze ingestion for lufthansa")
    monthly_bronze_lufthansa_ingestion()
    logger.info("Finished monthly bronze ingestion for lufthansa")

 
if __name__ == "__main__":
    main()
