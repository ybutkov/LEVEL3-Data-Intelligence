import sys
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

root_path = spark.conf.get("root_path")
if root_path and root_path not in sys.path:
    sys.path.insert(0, root_path)

from src.config.config_properties import get_ConfigProperties
from src.util.tables_utils import build_table_name
from src.util.tables_utils import build_full_table_name


CATALOG = spark.conf.get("catalog")
SILVER_SCHEMA = spark.conf.get("silver_schema")
GOLD_SCHEMA = spark.conf.get("gold_schema")
# Constants
MIN_DELAY_MINUTES = 15  # ignore delays less than 15 minutes


@dp.materialized_view(
    name=build_full_table_name(CATALOG, GOLD_SCHEMA, "gold_flight_metrics"),
    comment="Flight metrics with delay thresholds and completion flags"
)
def gold_flight_metrics():
    # cfg = get_ConfigProperties()
    
    # silver_schema = cfg.storage.silver_schema
    fact_flight_identity = build_full_table_name(CATALOG, SILVER_SCHEMA, "fact_flight_identity")
    fact_flight_status = build_full_table_name(CATALOG, SILVER_SCHEMA, "fact_flight_status")
    ref_dim_airport = build_full_table_name(CATALOG, SILVER_SCHEMA, "ref_dim_airport")
    dim_flight_status = build_full_table_name(CATALOG, SILVER_SCHEMA, "dim_flight_status")
    dim_time_status = build_full_table_name(CATALOG, SILVER_SCHEMA, "dim_time_status")
    
    flight_identity_df = spark.table(fact_flight_identity)
    flight_status_df = spark.table(fact_flight_status)
    
    flights_df = (
        flight_identity_df
        .join(
            flight_status_df.drop(
                "scheduled_departure_utc", "scheduled_arrival_utc",
                "scheduled_departure_local", "scheduled_arrival_local",
                "actual_departure_local", "actual_arrival_local",
                "estimated_departure_local", "estimated_departure_utc",
                "estimated_arrival_local", "estimated_arrival_utc",
                "source_file", "bronze_ingested_at", "ingest_run_id", "source_endpoint",
                "flight_status_description"
            ),
            on=["flight_key"],
            how="left"
        )
        .withColumn("departure_day_of_week", F.dayofweek(F.col("scheduled_departure_utc")))
        .withColumn("departure_day_name", F.date_format(F.col("scheduled_departure_utc"), "EEEE"))
        .withColumn("arrival_day_name", F.date_format(F.col("scheduled_arrival_utc"), "EEEE"))
        .withColumn("departure_hour", F.hour(F.col("scheduled_departure_utc")))
        .withColumn("is_weekend", F.when(F.dayofweek(F.col("scheduled_departure_utc")).isin(1, 7), 1).otherwise(0))
    )
    
    airport_dim_df = spark.table(ref_dim_airport).select(
        "airport_code",
        F.col("country_code").alias("airport_country_code"),
    )
    
    flight_status_dim = spark.table(dim_flight_status).select(
        "flight_status_code", "flight_status_description"
    )
    
    time_status_dim_dept = spark.table(dim_time_status).select(
        F.col("time_status_code").alias("dept_time_code"),
        F.col("time_status_description").alias("dept_time_description")
    )
    
    time_status_dim_arr = spark.table(dim_time_status).select(
        F.col("time_status_code").alias("arr_time_code"),
        F.col("time_status_description").alias("arr_time_description")
    )
    
    enriched = (
        flights_df
        .join(
            F.broadcast(airport_dim_df.alias("dep_airport_dim")),
            F.col("departure_airport_code") == F.col("dep_airport_dim.airport_code"),
            "left",
        )
        .withColumnRenamed("airport_country_code", "departure_country_code")
        .drop(F.col("dep_airport_dim.airport_code"))
        .join(
            F.broadcast(airport_dim_df.alias("arr_airport_dim")),
            F.col("arrival_airport_code") == F.col("arr_airport_dim.airport_code"),
            "left",
        )
        .withColumnRenamed("airport_country_code", "arrival_country_code")
        .drop(F.col("arr_airport_dim.airport_code"))
        .join(
            F.broadcast(flight_status_dim.alias("fs_dim")),
            flights_df.flight_status_code == F.col("fs_dim.flight_status_code"),
            "left",
        )
        .withColumn("flight_status_description", F.col("fs_dim.flight_status_description"))
        .drop(F.col("fs_dim.flight_status_code"), F.col("fs_dim.flight_status_description"))
        .join(
            F.broadcast(time_status_dim_dept),
            F.col("departure_time_status_code") == F.col("dept_time_code"),
            "left",
        )
        .withColumn("departure_time_status_description", F.col("dept_time_description"))
        .drop("dept_time_code", "dept_time_description")
        .join(
            F.broadcast(time_status_dim_arr),
            F.col("arrival_time_status_code") == F.col("arr_time_code"),
            "left",
        )
        .withColumn("arrival_time_status_description", F.col("arr_time_description"))
        .drop("arr_time_code", "arr_time_description")
        .withColumn("language_code", F.lit("EN"))
    )
    
    return (
        enriched
        .withColumn(
            "departure_delay_min",
            F.when(
                F.col("actual_departure_utc").isNotNull() & F.col("scheduled_departure_utc").isNotNull(),
                F.greatest(
                    F.lit(0),
                    (F.unix_timestamp("actual_departure_utc") - F.unix_timestamp("scheduled_departure_utc")) / 60.0
                )
            )
        )
        .withColumn(
            "arrival_delay_min",
            F.when(
                F.col("actual_arrival_utc").isNotNull() & F.col("scheduled_arrival_utc").isNotNull(),
                F.greatest(
                    F.lit(0),
                    (F.unix_timestamp("actual_arrival_utc") - F.unix_timestamp("scheduled_arrival_utc")) / 60.0
                )
            )
        )
        .withColumn(
            "flight_duration_min",
            F.when(
                F.col("actual_departure_utc").isNotNull() & F.col("actual_arrival_utc").isNotNull(),
                (F.unix_timestamp("actual_arrival_utc") - F.unix_timestamp("actual_departure_utc")) / 60.0
            )
        )
        .withColumn(
            "is_completed",
            F.when(F.col("actual_arrival_utc").isNotNull(), 1).otherwise(0)
        )
        .withColumn(
            "is_cancelled",
            F.when(F.col("flight_status_code") == "CA", 1).otherwise(0)
        )
        .withColumn(
            "is_delayed",
            F.when(
                (F.col("actual_departure_utc").isNotNull()) &
                (F.col("scheduled_departure_utc").isNotNull()) &
                ((F.unix_timestamp("actual_departure_utc") - F.unix_timestamp("scheduled_departure_utc")) / 60.0 >= MIN_DELAY_MINUTES),
                1
            ).otherwise(0)
        )
    )


@dp.materialized_view(
    name=build_full_table_name(CATALOG, GOLD_SCHEMA, "gold_airport_traffic"),
    comment="Airport traffic with movement-type detail and aggregate totals"
)
def gold_airport_traffic():
    flight_metrics = spark.table(build_full_table_name(CATALOG, GOLD_SCHEMA, "gold_flight_metrics"))
    
    dept_traffic_df = (
        flight_metrics
        .filter((F.col("departure_airport_code").isNotNull()) & (F.col("departure_country_code").isNotNull()))
        .groupBy("departure_airport_code", "departure_country_code", "departure_day_name")
        .agg(
            F.coalesce(F.count("*"), F.lit(0)).alias("total_events"),
            F.coalesce(F.sum("is_completed"), F.lit(0)).alias("total_completed"),
            F.coalesce(F.sum(F.when((F.col("is_delayed") == 1) & (F.col("is_completed") == 1), 1)), F.lit(0)).alias("total_delayed"),
            F.coalesce(F.sum(F.when(F.col("is_completed") == 1, F.col("departure_delay_min"))), F.lit(0)).alias("total_delay_minutes"),
        )
        .select(
            F.col("departure_airport_code").alias("airport_code"),
            F.col("departure_country_code").alias("country_code"),
            F.lit("DEPARTURE").alias("event_type"),
            F.col("departure_day_name").alias("day_name"),
            "total_events",
            "total_completed",
            "total_delayed",
            "total_delay_minutes",
        )
        .withColumn(
            "avg_delay_min",
            F.when(
                F.col("total_delayed") > 0,
                F.round(F.col("total_delay_minutes") / F.col("total_delayed"), 2)
            )
        )
        .withColumn(
            "delay_rate_pct",
            F.when(
                F.col("total_completed") > 0,
                F.round(F.col("total_delayed") * 100.0 / F.col("total_completed"), 2)
            ).otherwise(0)
        )
        .drop("total_delay_minutes")
    )
    
    arr_traffic_df = (
        flight_metrics
        .filter((F.col("arrival_airport_code").isNotNull()) & (F.col("arrival_country_code").isNotNull()))
        .groupBy("arrival_airport_code", "arrival_country_code", "arrival_day_name")
        .agg(
            F.coalesce(F.count("*"), F.lit(0)).alias("total_events"),
            F.coalesce(F.sum("is_completed"), F.lit(0)).alias("total_completed"),
            F.coalesce(F.sum(F.when((F.col("is_delayed") == 1) & (F.col("is_completed") == 1), 1)), F.lit(0)).alias("total_delayed"),
            F.coalesce(F.sum(F.when(F.col("is_completed") == 1, F.col("arrival_delay_min"))), F.lit(0)).alias("total_delay_minutes"),
        )
        .select(
            F.col("arrival_airport_code").alias("airport_code"),
            F.col("arrival_country_code").alias("country_code"),
            F.lit("ARRIVAL").alias("event_type"),
            F.col("arrival_day_name").alias("day_name"),
            "total_events",
            "total_completed",
            "total_delayed",
            "total_delay_minutes",
        )
        .withColumn(
            "avg_delay_min",
            F.when(
                F.col("total_delayed") > 0,
                F.round(F.col("total_delay_minutes") / F.col("total_delayed"), 2)
            )
        )
        .withColumn(
            "delay_rate_pct",
            F.when(
                F.col("total_completed") > 0,
                F.round(F.col("total_delayed") * 100.0 / F.col("total_completed"), 2)
            ).otherwise(0)
        )
        .drop("total_delay_minutes")
    )
    
    airport_traffic_df = dept_traffic_df.union(arr_traffic_df)
    
    # Aggregate airport-level totals across departure and arrival events
    airport_totals_df = (
        airport_traffic_df
        .groupBy("airport_code", "country_code", "day_name")
        .agg(
            F.sum("total_events").alias("total_events_overall"),
            F.sum("total_completed").alias("total_completed_overall"),
            F.sum("total_delayed").alias("total_delayed_overall"),
        )
    )
    
    # Join totals back to movement-type details
    return (
        airport_traffic_df
        .join(
            airport_totals_df,
            on=["airport_code", "country_code", "day_name"],
            how="left"
        )
    )

