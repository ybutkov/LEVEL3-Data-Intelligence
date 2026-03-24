from pyspark.sql.functions import try_to_timestamp, col, lit

LOCAL_TS_FORMAT = "yyyy-MM-dd'T'HH:mm"
UTC_TS_FORMAT = "yyyy-MM-dd'T'HH:mmX"


OPERATIONAL_TRANSFORMATION_MAP = {
    "op_fact_flight_status": [
        lambda df: df.withColumn(
            "scheduled_departure_local",
            try_to_timestamp(col("scheduled_departure_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "scheduled_departure_utc",
            try_to_timestamp(col("scheduled_departure_utc"), lit(UTC_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "actual_departure_local",
            try_to_timestamp(col("actual_departure_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "actual_departure_utc",
            try_to_timestamp(col("actual_departure_utc"), lit(UTC_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "estimated_departure_local",
            try_to_timestamp(col("estimated_departure_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "estimated_departure_utc",
            try_to_timestamp(col("estimated_departure_utc"), lit(UTC_TS_FORMAT)),
        ),

        lambda df: df.withColumn(
            "scheduled_arrival_local",
            try_to_timestamp(col("scheduled_arrival_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "scheduled_arrival_utc",
            try_to_timestamp(col("scheduled_arrival_utc"), lit(UTC_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "actual_arrival_local",
            try_to_timestamp(col("actual_arrival_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "actual_arrival_utc",
            try_to_timestamp(col("actual_arrival_utc"), lit(UTC_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "estimated_arrival_local",
            try_to_timestamp(col("estimated_arrival_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "estimated_arrival_utc",
            try_to_timestamp(col("estimated_arrival_utc"), lit(UTC_TS_FORMAT)),
        ),
    ]
}