from config.config_properties import get_ConfigProperties
from app.init_app import init_app
from config.endpoints import EndpointKeys
from util.json_utils import get_value_by_path
from config.endpoints import get_endpoint_config
from app.logger import get_logger

from services.ingestion_service import get_full_save_path
from services.ingestion_service import get_and_save_all_pages
from services.ingestion_service import build_url_for_endpoint, fetch_data
from services.storage_service import load_json_to_bronze_autoloader

def monthly_job():
  # get_and_save_allget_Conconfig_propertiesfigProperties_pages(cfg.EndpointKeys.COUNTRIES, limit=100, 
  #                       time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_MONTH_FORMAT))
  # load_data_to_bronze(cfg.EndpointKeys.COUNTRIES)

  # get_and_save_all_pages(cfg.EndpointKeys.AIRPORTS, limit=100, 
  #                      time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_MONTH_FORMAT))
  # load_data_to_bronze(cfg.EndpointKeys.AIRPORTS)

  # get_and_save_all_pages(cfg.EndpointKeys.AIRLINES, limit=100, 
  #                        time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_DAYLY_FORMAT))
  # get_and_save_all_pages(cfg.EndpointKeys.AIRLINES, limit=100, 
  #                        time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_MONTH_FORMAT))
  # load_data_to_bronze(cfg.EndpointKeys.AIRLINES)
  pass

def test_config(config):
    print(config.api.base_url)
    print(config.format.ts_monthly_format)
    print(config.api.version)
    print(config.secrets.secret_scope)
    print(config.secrets.password_key)

def test_fetch():
    page_query_params = {
        "limit": 30,
        "offset": 100}
    endpoint = EndpointKeys.COUNTRIES
    url = build_url_for_endpoint(endpoint, page_query_params)
    response = fetch_data(
            url=url,
            query_params=page_query_params,
        )
    config = get_endpoint_config(endpoint)
    print(config.total_count_path)
    print(get_value_by_path(response.json(), config.total_count_path))
    print(get_full_save_path(endpoint))
    print(response.json())

def test_get_and_save_all_pages():
    get_and_save_all_pages(EndpointKeys.COUNTRIES, limit=100, time_period="monthly")
    # get_and_save_all_pages(EndpointKeys.CITIES, limit=100, time_period="monthly")
    # get_and_save_all_pages(EndpointKeys.AIRPORTS, limit=100, time_period="monthly")
    # get_and_save_all_pages(EndpointKeys.AIRLINES, limit=100, time_period="monthly")
    # get_and_save_all_pages(EndpointKeys.AIRCRAFT, limit=100, time_period="monthly")

def test_logger():
    logger = get_logger("test_logger")

    logger.debug("TEST")
    logger.info("TEST")
    logger.warning("TEST")
    logger.error("TEST")
    logger.critical("TEST")

def test_bronze_autoloader():
    logger = get_logger("test_bronze_autoloader")
    logger.info("test_bronze_autoloader")
    get_and_save_all_pages(EndpointKeys.COUNTRIES, limit=50, time_period="daily")
    # get_and_save_all_pages(EndpointKeys.COUNTRIES, limit=100, time_period="monthly")
    load_json_to_bronze_autoloader(EndpointKeys.COUNTRIES, time_period="daily")

def main():
    init_app()
    # config = get_ConfigProperties()
    # logger = get_logger("test_logger")
    # test_logger()
    test_bronze_autoloader()
    # test_config(config)
    # test_fetch()
    test_get_and_save_all_pages()
    # get_and_save_all_pages(EndpointKeys.COUNTRIES, limit=100, time_period="dayly")
    
 
if __name__ == "__main__":
    main()
