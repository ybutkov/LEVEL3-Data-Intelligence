from src.app.logger import get_logger
from src.config.settings import get_settings
from pyspark.sql import SparkSession


logger = get_logger(__name__)


def run_operations_gold(spark, cfg):
    logger.info("Start operations gold job")

    silver_table = f"{cfg.storage.catalog}.{cfg.storage.silver_schema}.op_fact_flight_status"

    df = spark.table(silver_table)
    row_count = df.count()

    logger.info(f"Read silver table: {silver_table}, rows={row_count}")

    # TODO:
    # Build gold tables here

    logger.info("Finish operations gold job")


if __name__ == "__main__":

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    cfg = get_settings()

    run_operations_gold(spark, cfg)
