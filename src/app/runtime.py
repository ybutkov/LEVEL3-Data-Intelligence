from src.app.logger import get_logger
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


def is_spark_session_alive(spark: SparkSession)->bool:
    try:
        spark.sql("SELECT 1").collect()
        return True
    except Exception:
        logger = get_logger()
        logger.exception("Spark session expired")
        raise

def get_spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()

    if spark is None:
        raise RuntimeError("No active Spark session.")
    is_spark_session_alive(spark)
    return spark

def get_dbutils() -> DBUtils:
  return DBUtils(get_spark())
  