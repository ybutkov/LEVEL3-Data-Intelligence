from functools import lru_cache

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from app.logger import get_logger

def is_spark_session_alive(spark: SparkSession)->bool:
    try:
        spark.sql("SELECT 1").collect()
        return True
    except Exception:
        logger = get_logger()
        logger.exception("Spark session expired")
        raise

# @lru_cache(maxsize=1)
def get_spark() -> SparkSession:
    spark = SparkSession.getActiveSession()
    # if spark is None:
    #     spark = SparkSession.builder.getOrCreate()
    if spark is None:
        raise RuntimeError("No active Spark session.")
    # if not is_spark_session_alive(spark):
    #     raise RuntimeError("Spark session expired.")
    is_spark_session_alive(spark)
    return spark

# @lru_cache(maxsize=1)
def get_dbutils() -> DBUtils:
  return DBUtils(get_spark())
  