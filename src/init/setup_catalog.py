import sys, os, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args, _ = parser.parse_known_args()
if args.root_path and args.root_path not in sys.path:
    sys.path.insert(0, args.root_path)

from pyspark.sql import SparkSession
from src.app.logger import get_logger

spark = SparkSession.getActiveSession()

logger = get_logger("lufthansa_init_setup")

logger.info("INITIALIZING LUFTHANSA CATALOG & SCHEMAS")

try:
    spark.sql("CREATE CATALOG IF NOT EXISTS lufthansa_level")
    logger.info("✓ Catalog: lufthansa_level")
except Exception as e:
    logger.warning(f"Catalog creation note: {e}")

spark.sql("CREATE SCHEMA IF NOT EXISTS lufthansa_level.bronze")
logger.info("✓ Schema: lufthansa_level.bronze")

spark.sql("CREATE SCHEMA IF NOT EXISTS lufthansa_level.silver")
logger.info("✓ Schema: lufthansa_level.silver")

spark.sql("CREATE SCHEMA IF NOT EXISTS lufthansa_level.silver_audit")
logger.info("✓ Schema: lufthansa_level.silver_audit")

spark.sql("CREATE SCHEMA IF NOT EXISTS lufthansa_level.gold")
logger.info("✓ Schema: lufthansa_level.gold")
