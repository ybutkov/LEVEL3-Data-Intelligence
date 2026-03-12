from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import config.config as cfg
from utils.ingestion_utils import *

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

print("Monthly Job start")

# for endpoint in cfg.LIST_ENDPOINTS:
#     print(f"Loading data for endpoint {endpoint}")
#     get_and_save_all_pages(endpoint, limit=100, time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_MONTH_FORMAT))

# get_and_save_all_pages(cfg.EndpointKeys.AIRPORTS, limit=100, time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_MONTH_FORMAT))
get_and_save_all_pages(cfg.EndpointKeys.COUNTRIES, limit=100, time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_MONTH_FORMAT))
# get_and_save_all_pages(cfg.EndpointKeys.CITIES, limit=100, time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_MONTH_FORMAT))
# get_and_save_all_pages(cfg.EndpointKeys.AIRLINES, limit=100, time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_MONTH_FORMAT))
# get_and_save_all_pages(cfg.EndpointKeys.AIRCRAFT, limit=100, time_dir_format=cfg.PROFILE.get(cfg.ProfileKeys.TS_MONTH_FORMAT))


print("Monthly Job finish")
