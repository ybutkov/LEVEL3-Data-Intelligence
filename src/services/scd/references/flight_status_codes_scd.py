from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType

FLIGHT_STATUS_CODES = {
    "SC": "Scheduled",
    "DL": "Delayed",
    "DP": "Departed",
    "DV": "Diverted",
    "LND": "Landed",
    "NO": "No Operation",
    "OK": "On Time",
    "RA": "Rerouted",
    "RP": "Rerouted and Delayed",
    "CA": "Cancelled",
    "BD": "Boarding",
    "CK": "Check-In",
}

@dp.table
def dim_flight_status():
    data = [
        (code, description) 
        for code, description in FLIGHT_STATUS_CODES.items()
    ]
    schema = StructType([
        StructField("flight_status_code", StringType(), False),
        StructField("flight_status_description", StringType(), False),
    ])
    return spark.createDataFrame(data, schema=schema)

dp.create_streaming_table("silver.dim_flight_status")
dp.create_auto_cdc_flow(
    target="silver.dim_flight_status",
    source="dim_flight_status",
    keys=["flight_status_code"],
    stored_as_scd_type=1,
)
