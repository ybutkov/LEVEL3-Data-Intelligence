import sys, os, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args, _ = parser.parse_known_args()
if args.root_path and args.root_path not in sys.path:
    sys.path.insert(0, args.root_path)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.app.logger import get_logger

spark = SparkSession.getActiveSession()
logger = get_logger("lufthansa_init_references")

logger.info("LOADING REFERENCE DIMENSIONS")

# ============================================================
# TIME STATUS CODES
# ============================================================

time_data = [
    ("ON", "On Time"),
    ("OT", "On Time"),
    ("DL", "Delayed"),
    ("AR", "Arrived"),
    ("BD", "Boarding"),
    ("CK", "Check-In"),
    ("DP", "Departed"),
    ("GD", "Gate Delay"),
    ("FE", "Forward Estimeted"),
    ("NO", "No info"),
]

time_schema = StructType([
    StructField("time_status_code", StringType(), False),
    StructField("time_status_description", StringType(), False),
])

spark.createDataFrame(time_data, schema=time_schema) \
    .write.mode("overwrite") \
    .format("delta") \
    .saveAsTable("lufthansa_level.silver.dim_time_status")

logger.info("dim_time_status loaded")

# ============================================================
# FLIGHT STATUS CODES
# ============================================================

flight_data = [
    ("SC", "Scheduled"),
    ("DL", "Delayed"),
    ("DP", "Departed"),
    ("DV", "Diverted"),
    ("LND", "Landed"),
    ("LD", "Landed"),
    ("NO", "No Operation"),
    ("OK", "On Time"),
    ("RA", "Rerouted"),
    ("RP", "Rerouted and Delayed"),
    ("CA", "Cancelled"),
    ("CD", "Cancelled"),
    ("BD", "Boarding"),
    ("CK", "Check-In"),
    ("NA", "No Arrival?"),
    ("RT", "Returned"),
]

flight_schema = StructType([
    StructField("flight_status_code", StringType(), False),
    StructField("flight_status_description", StringType(), False),
])

spark.createDataFrame(flight_data, schema=flight_schema) \
    .write.mode("overwrite") \
    .format("delta") \
    .saveAsTable("lufthansa_level.silver.dim_flight_status")

logger.info("dim_flight_status loaded")

# ============================================================
# SERVICE TYPE CODES
# ============================================================

service_data = [
    ("F", "Flight"),
    ("C", "Charter Flight"),
    ("H", "Charter Flight"),
    ("S", "Short Haul"),
]

service_schema = StructType([
    StructField("service_type_code", StringType(), False),
    StructField("service_type_description", StringType(), False),
])

spark.createDataFrame(service_data, schema=service_schema) \
    .write.mode("overwrite") \
    .format("delta") \
    .saveAsTable("lufthansa_level.silver.dim_service_type")

logger.info("dim_service_type loaded")

time_count = spark.table("lufthansa_level.silver.dim_time_status").count()
logger.info(f"Time Status: {time_count} records")

flight_count = spark.table("lufthansa_level.silver.dim_flight_status").count()
logger.info(f"Flight Status: {flight_count} records")

service_count = spark.table("lufthansa_level.silver.dim_service_type").count()
logger.info(f"Service Type: {service_count} records")
