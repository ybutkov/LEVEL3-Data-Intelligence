from functools import lru_cache

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

@lru_cache(maxsize=1)
def get_spark() -> SparkSession:
    spark = SparkSession.getActiveSesion()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark

@lru_cache(maxsize=1)
def get_dbutils():
  return DBUtils(get_spark())
  