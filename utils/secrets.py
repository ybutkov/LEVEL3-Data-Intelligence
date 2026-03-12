from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import config.config as cfg

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def get_secret(key):
    return dbutils.secrets.get(cfg.SECRET_SCOPE, key)

def get_proxy_password():
    return get_secret(cfg.PROXY_PASSWORD_KEY)
