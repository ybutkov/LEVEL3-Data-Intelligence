from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType

TIME_STATUS_CODES = {
    "ON": "On Time",
    "DL": "Delayed",
    "AR": "Arrived",
    "BD": "Boarding",
    "CK": "Check-In",
    "DP": "Departed",
    "GD": "Gate Delay",
}

@dp.table
def dim_time_status():
    data = [
        (code, description) 
        for code, description in TIME_STATUS_CODES.items()
    ]
    schema = StructType([
        StructField("time_status_code", StringType(), False),
        StructField("time_status_description", StringType(), False),
    ])
    return spark.createDataFrame(data, schema=schema)

dp.create_streaming_table("silver.dim_time_status")
dp.create_auto_cdc_flow(
    target="silver.dim_time_status",
    source="dim_time_status",
    keys=["time_status_code"],
    stored_as_scd_type=1,
)
