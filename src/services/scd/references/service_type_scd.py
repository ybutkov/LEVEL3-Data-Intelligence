from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType

SERVICE_TYPE_CODES = {
    "F": "Flight",
    "C": "Charter Flight",
    "H": "Charter Flight",
    "S": "Short Haul",
}

@dp.table
def dim_service_type():
    data = [
        (code, description) 
        for code, description in SERVICE_TYPE_CODES.items()
    ]
    schema = StructType([
        StructField("service_type_code", StringType(), False),
        StructField("service_type_description", StringType(), False),
    ])
    return spark.createDataFrame(data, schema=schema)

dp.create_streaming_table("silver.dim_service_type")
dp.create_auto_cdc_flow(
    target="silver.dim_service_type",
    source="dim_service_type",
    keys=["service_type_code"],
    stored_as_scd_type=1,
)
