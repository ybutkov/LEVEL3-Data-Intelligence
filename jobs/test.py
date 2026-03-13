from utils.ingestion_utils import *
from app.config import get_config
from app.init_app import init_app


def monthly_job():
  # get_and_save_all_pages(cfg.EndpointKeys.COUNTRIES, limit=100, 
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

def main():
    init_app()
    config = get_config()
    test_config(config)
 
if __name__ == "__main__":
    main()
