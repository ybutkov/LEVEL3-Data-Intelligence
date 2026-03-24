from pyspark.sql.functions import try_to_timestamp, col, lit

LOCAL_TS_FORMAT = "yyyy-MM-dd'T'HH:mm"
UTC_TS_FORMAT = "yyyy-MM-dd'T'HH:mmX"

OPERATIONAL_TRANSFORMATION_MAP = {
    "op_fact_flight_status": [
        lambda df: df.withColumn(
            "sched_dep_local",
            try_to_timestamp(col("sched_dep_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "best_dep_local",
            try_to_timestamp(col("best_dep_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "sched_arr_local",
            try_to_timestamp(col("sched_arr_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "best_arr_local",
            try_to_timestamp(col("best_arr_local"), lit(LOCAL_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "sched_dep_utc",
            try_to_timestamp(col("sched_dep_utc"), lit(UTC_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "best_dep_utc",
            try_to_timestamp(col("best_dep_utc"), lit(UTC_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "sched_arr_utc",
            try_to_timestamp(col("sched_arr_utc"), lit(UTC_TS_FORMAT)),
        ),
        lambda df: df.withColumn(
            "best_arr_utc",
            try_to_timestamp(col("best_arr_utc"), lit(UTC_TS_FORMAT)),
        ),
    ]
}
