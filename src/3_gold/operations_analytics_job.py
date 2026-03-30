import sys, os, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args,_ = parser.parse_known_args()
if args.root_path and args.root_path not in sys.path:
    sys.path.insert(0, args.root_path)

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

from src.app.logger import get_logger
from src.app.init_app import init_app
from src.config.config_properties import get_ConfigProperties

logger = get_logger(__name__)


def build_table_name(cfg, schema_name: str, table_name: str) -> str:
    return f"{cfg.storage.catalog}.{schema_name}.{table_name}"


def run_operations_analytics(spark, cfg):
    logger.info("Start operations analytics gold job")
    
    MIN_DELAY_MINUTES = 15  # ignore delays less than 15 minutes

    silver_schema = cfg.storage.silver_schema
    gold_schema = cfg.storage.gold_schema

    # Silver table references
    fact_flight_identity = build_table_name(cfg, silver_schema, "fact_flight_identity")
    fact_flight_status = build_table_name(cfg, silver_schema, "fact_flight_status")
    ref_dim_airport = build_table_name(cfg, silver_schema, "ref_dim_airport")
    ref_dim_aircraft = build_table_name(cfg, silver_schema, "ref_dim_aircraft")
    ref_dim_airline = build_table_name(cfg, silver_schema, "ref_dim_airline")
    ref_airport_names = build_table_name(cfg, silver_schema, "ref_airport_names_flat")
    ref_airline_names = build_table_name(cfg, silver_schema, "ref_airline_names_flat")
    ref_aircraft_names = build_table_name(cfg, silver_schema, "ref_aircraft_names_flat")
    ref_country_names = build_table_name(cfg, silver_schema, "ref_country_names_flat")
    dim_flight_status = build_table_name(cfg, silver_schema, "dim_flight_status")
    dim_time_status = build_table_name(cfg, silver_schema, "dim_time_status")

    # Gold table references
    gold_enriched_flights = build_table_name(cfg, gold_schema, "gold_enriched_flights")
    gold_flight_metrics = build_table_name(cfg, gold_schema, "gold_flight_metrics")
    gold_aircraft_utilization = build_table_name(cfg, gold_schema, "gold_aircraft_utilization")
    gold_airport_traffic = build_table_name(cfg, gold_schema, "gold_airport_traffic")
    gold_airline_performance = build_table_name(cfg, gold_schema, "gold_airline_performance")

    logger.info("Reading silver fact tables")
    flight_identity_df = spark.table(fact_flight_identity)
    flight_status_df = spark.table(fact_flight_status)

    flights_df = (
        flight_identity_df
        .join(
            flight_status_df.drop(
                # Timestamp duplicates from identity table
                "scheduled_departure_utc", 
                "scheduled_arrival_utc",
                "scheduled_departure_local",
                "scheduled_arrival_local",
                # Actual timestamps not in identity
                "actual_departure_local",
                "actual_arrival_local",
                # Estimated timestamps not in identity
                "estimated_departure_local",
                "estimated_departure_utc",
                "estimated_arrival_local",
                "estimated_arrival_utc",
                # Metadata duplicates
                "source_file",
                "bronze_ingested_at",
                "ingest_run_id",
                "source_endpoint",
                # Flight status description to use dimension table version
                "flight_status_description"
            ),
            on=["flight_key"],
            how="left"
        )
        # Add time-based analytics columns
        .withColumn("departure_day_of_week", F.dayofweek(F.col("scheduled_departure_utc")))
        .withColumn("departure_day_name", F.date_format(F.col("scheduled_departure_utc"), "EEEE"))
        .withColumn("departure_hour", F.hour(F.col("scheduled_departure_utc")))
        .withColumn("is_weekend", F.when(F.dayofweek(F.col("scheduled_departure_utc")).isin(1, 7), 1).otherwise(0))
    )

    logger.info("Building reference lookup tables - keys only (translations in silver)")
    
    airport_dim_df = (
        spark.table(ref_dim_airport)
        .select(
            "airport_code",
            F.col("country_code").alias("airport_country_code"),
        )
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
    
    logger.info("Broadcasting dimension tables for optimized joins")
    
    enriched_flights_df = (
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
            F.col("flight_status_code") == F.col("fs_dim.flight_status_code"),
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

    enriched_flights_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(gold_enriched_flights)
    logger.info(f"Saved enriched flights table (keys only): {gold_enriched_flights}")

    logger.info("Building flight metrics table with industry-standard delay thresholds")
    
    flight_metrics_df = (
        enriched_flights_df
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

    flight_metrics_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(gold_flight_metrics)
    logger.info(f"Saved flight metrics table: {gold_flight_metrics}")

    logger.info("Building aircraft utilization table")
    
    aircraft_util_df = (
        flight_metrics_df
        .filter(F.col("aircraft_code").isNotNull())
        .groupBy("aircraft_code")
        .agg(
            F.count("*").alias("total_flights"),
            F.sum("is_completed").alias("completed_flights"),
            F.sum("is_cancelled").alias("cancelled_flights"),
            F.round(F.avg(F.when(F.col("is_completed") == 1, F.col("flight_duration_min"))), 2).alias("avg_flight_duration_min"),
            F.sum(F.when((F.col("is_delayed") == 1) & (F.col("is_completed") == 1), F.col("departure_delay_min"))).alias("total_departure_delay_minutes"),
            F.sum(F.when((F.col("is_delayed") == 1) & (F.col("is_completed") == 1), 1)).alias("delayed_flights_count"),
        )
        .withColumn(
            "utilization_rate_pct",
            F.round(F.col("completed_flights") * 100.0 / F.col("total_flights"), 2)
        )
        .withColumn(
            "delay_rate_pct",
            F.when(
                F.col("completed_flights") > 0,
                F.round(F.col("delayed_flights_count") * 100.0 / F.col("completed_flights"), 2)
            )
        )
        .withColumn(
            "avg_departure_delay_min",
            F.when(
                F.col("delayed_flights_count") > 0,
                F.round(F.col("total_departure_delay_minutes") / F.col("delayed_flights_count"), 2)
            )
        )
        .drop("total_departure_delay_minutes")
    )

    aircraft_util_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(gold_aircraft_utilization)
    logger.info(f"Saved aircraft utilization table: {gold_aircraft_utilization}")

    logger.info("Building airport traffic table with mathematically accurate delay averages")
    
    dept_traffic_df = (
        flight_metrics_df
        .filter((F.col("departure_airport_code").isNotNull()) & (F.col("departure_country_code").isNotNull()))
        .groupBy("departure_airport_code", "departure_country_code")
        .agg(
            F.count("*").alias("total_departures"),
            F.sum("is_completed").alias("completed_departures"),
            F.sum(F.when((F.col("is_delayed") == 1) & (F.col("is_completed") == 1), 1)).alias("delayed_departures"),
            F.sum(F.when(F.col("is_completed") == 1, F.col("departure_delay_min"))).alias("total_departure_delay_minutes"),
        )
        .select(
            F.col("departure_airport_code").alias("airport_code"),
            F.col("departure_country_code").alias("country_code"),
            F.lit("DEPARTURE").alias("movement_type"),
            "total_departures",
            "completed_departures",
            "delayed_departures",
            "total_departure_delay_minutes",
        )
        .withColumn(
            "avg_delay_min",
            F.when(
                F.col("delayed_departures") > 0,
                F.round(F.col("total_departure_delay_minutes") / F.col("delayed_departures"), 2)
            )
        )
        .withColumn(
            "delay_rate_pct",
            F.when(
                F.col("completed_departures") > 0,
                F.round(F.col("delayed_departures") * 100.0 / F.col("completed_departures"), 2)
            )
        )
        .drop("total_departure_delay_minutes")
    )

    arr_traffic_df = (
        flight_metrics_df
        .filter((F.col("arrival_airport_code").isNotNull()) & (F.col("arrival_country_code").isNotNull()))
        .groupBy("arrival_airport_code", "arrival_country_code")
        .agg(
            F.count("*").alias("total_arrivals"),
            F.sum("is_completed").alias("completed_arrivals"),
            F.sum(F.when((F.col("is_delayed") == 1) & (F.col("is_completed") == 1), 1)).alias("delayed_arrivals"),
            F.sum(F.when(F.col("is_completed") == 1, F.col("arrival_delay_min"))).alias("total_arrival_delay_minutes"),
        )
        .select(
            F.col("arrival_airport_code").alias("airport_code"),
            F.col("arrival_country_code").alias("country_code"),
            F.lit("ARRIVAL").alias("movement_type"),
            "total_arrivals",
            "completed_arrivals",
            "delayed_arrivals",
            "total_arrival_delay_minutes",
        )
        .withColumn(
            "avg_delay_min",
            F.when(
                F.col("delayed_arrivals") > 0,
                F.round(F.col("total_arrival_delay_minutes") / F.col("delayed_arrivals"), 2)
            )
        )
        .withColumn(
            "delay_rate_pct",
            F.when(
                F.col("completed_arrivals") > 0,
                F.round(F.col("delayed_arrivals") * 100.0 / F.col("completed_arrivals"), 2)
            )
        )
        .drop("total_arrival_delay_minutes")
    )

    airport_traffic_df = dept_traffic_df.union(arr_traffic_df)

    # Aggregate airport-level totals across departure and arrival movements and join back
    airport_totals_df = (
        airport_traffic_df
        .groupBy("airport_code", "country_code")
        .agg(
            F.sum("total_departures").alias("total_movements"),
            F.sum("completed_departures").alias("total_completed"),
            F.sum("delayed_departures").alias("total_delayed"),
        )
    )

    # Join totals back to movement-type details
    airport_traffic_df = (
        airport_traffic_df
        .join(
            airport_totals_df,
            on=["airport_code", "country_code"],
            how="left"
        )
    )

    airport_traffic_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(gold_airport_traffic)
    logger.info(f"Saved airport traffic table: {gold_airport_traffic}")

    logger.info("Building airline performance table")
    
    airline_perf_df = (
        flight_metrics_df
        .filter((F.col("marketing_airline_id").isNotNull()) & (F.col("operating_airline_id").isNotNull()))
        .groupBy(
            "marketing_airline_id",
            "operating_airline_id",
        )
        .agg(
            F.count("*").alias("total_flights"),
            F.sum("is_completed").alias("completed_flights"),
            F.sum("is_cancelled").alias("cancelled_flights"),
            F.sum(F.when((F.col("is_delayed") == 1) & (F.col("is_completed") == 1), 1)).alias("delayed_departure_flights"),
            F.sum(F.when((F.col("arrival_delay_min").isNotNull()) & (F.col("arrival_delay_min") > 0) & (F.col("is_completed") == 1), 1)).alias("delayed_arrival_flights"),
            F.sum(F.when((F.col("is_delayed") == 1) & (F.col("is_completed") == 1), F.col("departure_delay_min"))).alias("total_departure_delay_minutes"),
            F.sum(F.when((F.col("arrival_delay_min").isNotNull()) & (F.col("arrival_delay_min") > 0) & (F.col("is_completed") == 1), F.col("arrival_delay_min"))).alias("total_arrival_delay_minutes"),
            F.round(F.avg(F.when(F.col("is_completed") == 1, F.col("flight_duration_min"))), 2).alias("avg_flight_duration_min"),
        )
        .withColumn(
            "on_time_rate_pct",
            F.when(
                F.col("completed_flights") > 0,
                F.round((F.col("completed_flights") - F.col("delayed_departure_flights")) * 100.0 / F.col("completed_flights"), 2)
            )
        )
        .withColumn(
            "cancellation_rate_pct",
            F.when(
                F.col("total_flights") > 0,
                F.round(F.col("cancelled_flights") * 100.0 / F.col("total_flights"), 2)
            )
        )
        .withColumn(
            "avg_departure_delay_min",
            F.when(
                F.col("delayed_departure_flights") > 0,
                F.round(F.col("total_departure_delay_minutes") / F.col("delayed_departure_flights"), 2)
            )
        )
        .withColumn(
            "avg_arrival_delay_min",
            F.when(
                F.col("delayed_arrival_flights") > 0,
                F.round(F.col("total_arrival_delay_minutes") / F.col("delayed_arrival_flights"), 2)
            )
        )
        .drop("total_departure_delay_minutes", "total_arrival_delay_minutes")

    airline_perf_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(gold_airline_performance)
    logger.info(f"Saved airline performance table: {gold_airline_performance}")

    logger.info("Finish operations analytics gold job")


if __name__ == "__main__":

    init_app()
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    cfg = get_ConfigProperties()

    run_operations_analytics(spark, cfg)
