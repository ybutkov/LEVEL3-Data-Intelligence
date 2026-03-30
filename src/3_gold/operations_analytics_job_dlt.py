import sys
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

root_path = spark.conf.get("root_path")
if root_path and root_path not in sys.path:
    sys.path.insert(0, root_path)

from src.config.config_properties import get_ConfigProperties
from src.util.tables_utils import build_table_name

# Constants
MIN_DELAY_MINUTES = 15  # ignore delays less than 15 minutes


@dp.materialized_view(
    name="gold_enriched_flights",
    comment="Enriched flights with dimension lookups and time-based analytics columns"
)
def gold_enriched_flights():
    cfg = get_ConfigProperties()
    
    silver_schema = cfg.storage.silver_schema
    fact_flight_identity = build_table_name(cfg, silver_schema, "fact_flight_identity")
    fact_flight_status = build_table_name(cfg, silver_schema, "fact_flight_status")
    ref_dim_airport = build_table_name(cfg, silver_schema, "ref_dim_airport")
    dim_flight_status = build_table_name(cfg, silver_schema, "dim_flight_status")
    dim_time_status = build_table_name(cfg, silver_schema, "dim_time_status")
    
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
    
    return (
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


@dp.materialized_view(
    name="gold_flight_metrics",
    comment="Flight metrics with delay thresholds and completion flags"
)
def gold_flight_metrics():
    enriched = spark.table("gold_enriched_flights")
    
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
    name="gold_aircraft_utilization",
    comment="Aircraft-level utilization metrics with delay severity"
)
def gold_aircraft_utilization():
    cfg = get_ConfigProperties()
    gold_schema = cfg.storage.gold_schema
    
    flight_metrics = spark.table("gold_flight_metrics")
    
    return (
        flight_metrics
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


@dp.materialized_view(
    name="gold_airport_traffic",
    comment="Airport traffic with movement-type detail and aggregate totals"
)
def gold_airport_traffic():
    flight_metrics = spark.table("gold_flight_metrics")
    
    dept_traffic_df = (
        flight_metrics
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
        .withColumn(
            "total_delayed",
            F.col("delayed_departures")
        )
        .drop("total_departure_delay_minutes")
    )
    
    arr_traffic_df = (
        flight_metrics
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
        .withColumn(
            "total_delayed",
            F.col("delayed_arrivals")
        )
        .drop("total_arrival_delay_minutes")
    )
    
    airport_traffic_df = dept_traffic_df.union(arr_traffic_df)
    
    # Aggregate airport-level totals across departure and arrival movements
    airport_totals_df = (
        airport_traffic_df
        .groupBy("airport_code", "country_code")
        .agg(
            F.sum(F.when(F.col("movement_type") == "DEPARTURE", F.col("total_departures")).otherwise(F.col("total_arrivals"))).alias("total_movements"),
            F.sum(F.when(F.col("movement_type") == "DEPARTURE", F.col("completed_departures")).otherwise(F.col("completed_arrivals"))).alias("total_completed"),
            F.sum("total_delayed").alias("total_delayed"),
        )
    )
    
    # Join totals back to movement-type details
    return (
        airport_traffic_df
        .join(
            airport_totals_df,
            on=["airport_code", "country_code"],
            how="left"
        )
    )


@dp.materialized_view(
    name="gold_airline_performance",
    comment="Airline performance with separate departure and arrival delay metrics"
)
def gold_airline_performance():
    flight_metrics = spark.table("gold_flight_metrics")
    
    return (
        flight_metrics
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
    )


@dp.materialized_view(
    name="gold_flight_status_latest",
    comment="Latest flight status for each flight with dimension enrichment"
)
def gold_flight_status_latest():
    cfg = get_ConfigProperties()
    
    silver_schema = cfg.storage.silver_schema
    op_fact_flight_status = build_table_name(cfg, silver_schema, "op_fact_flight_status")
    ref_dim_airport = build_table_name(cfg, silver_schema, "ref_dim_airport")
    ref_airport_names = build_table_name(cfg, silver_schema, "ref_airport_names_flat")
    ref_airline_names = build_table_name(cfg, silver_schema, "ref_airline_names_flat")
    ref_country_names = build_table_name(cfg, silver_schema, "ref_country_names_flat")
    
    flights_df = spark.table(op_fact_flight_status)
    
    # Get latest status for each flight
    flight_window = Window.partitionBy(
        "marketing_airline_id",
        "marketing_flight_number",
        "departure_airport_code",
        "arrival_airport_code",
        "scheduled_departure_utc",
    ).orderBy(
        F.when(F.col("actual_arrival_utc").isNotNull(), F.lit(1))
         .when(F.col("actual_departure_utc").isNotNull(), F.lit(2))
         .when(F.col("estimated_arrival_utc").isNotNull(), F.lit(3))
         .when(F.col("estimated_departure_utc").isNotNull(), F.lit(4))
         .otherwise(F.lit(5)),
        F.col("processed_at").desc(),
    )
    
    latest_df = (
        flights_df
        .withColumn("rn", F.row_number().over(flight_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    
    # Prepare name lookups (English only)
    airline_names_df = (
        spark.table(ref_airline_names)
        .filter(F.col("language_code") == "EN")
        .select(
            F.col("airline_id").alias("ref_marketing_airline_id"),
            F.col("airline_name"),
        )
    )
    
    airport_names_df = (
        spark.table(ref_airport_names)
        .filter(F.col("language_code") == "EN")
        .select("airport_code", "airport_name")
    )
    
    airport_dim_df = (
        spark.table(ref_dim_airport)
        .select(
            F.col("airport_code").alias("dim_airport_code"),
            F.col("country_code").alias("dim_country_code"),
        )
    )
    
    country_names_df = (
        spark.table(ref_country_names)
        .filter(F.col("language_code") == "EN")
        .select(
            F.col("country_code").alias("dim_country_code"),
            F.col("country_name"),
        )
    )
    
    return (
        latest_df
        .join(
            airline_names_df,
            latest_df.marketing_airline_id == airline_names_df.ref_marketing_airline_id,
            "left",
        )
        .drop("ref_marketing_airline_id")
        .join(
            airport_names_df.alias("dep_airport_names"),
            F.col("departure_airport_code") == F.col("dep_airport_names.airport_code"),
            "left",
        )
        .withColumnRenamed("airport_name", "departure_airport_name")
        .drop(F.col("dep_airport_names.airport_code"))
        .join(
            airport_names_df.alias("arr_airport_names"),
            F.col("arrival_airport_code") == F.col("arr_airport_names.airport_code"),
            "left",
        )
        .withColumnRenamed("airport_name", "arrival_airport_name")
        .drop(F.col("arr_airport_names.airport_code"))
        .join(
            airport_dim_df.alias("dep_airport_dim"),
            F.col("departure_airport_code") == F.col("dep_airport_dim.dim_airport_code"),
            "left",
        )
        .withColumnRenamed("dim_country_code", "departure_country_code")
        .drop(F.col("dep_airport_dim.dim_airport_code"))
        .join(
            airport_dim_df.alias("arr_airport_dim"),
            F.col("arrival_airport_code") == F.col("arr_airport_dim.dim_airport_code"),
            "left",
        )
        .withColumnRenamed("dim_country_code", "arrival_country_code")
        .drop(F.col("arr_airport_dim.dim_airport_code"))
        .join(
            country_names_df.alias("dep_country_names"),
            F.col("departure_country_code") == F.col("dep_country_names.dim_country_code"),
            "left",
        )
        .withColumnRenamed("country_name", "departure_country_name")
        .drop(F.col("dep_country_names.dim_country_code"))
        .join(
            country_names_df.alias("arr_country_names"),
            F.col("arrival_country_code") == F.col("arr_country_names.dim_country_code"),
            "left",
        )
        .withColumnRenamed("country_name", "arrival_country_name")
        .drop(F.col("arr_country_names.dim_country_code"))
    )


@dp.materialized_view(
    name="gold_airline_route_performance",
    comment="Route-level airline performance with delay metrics"
)
def gold_airline_route_performance():
    latest = spark.table("gold_flight_status_latest")
    
    return (
        latest
        .withColumn(
            "departure_delay_min",
            (
                F.unix_timestamp("actual_departure_utc")
                - F.unix_timestamp("scheduled_departure_utc")
            ) / 60.0
        )
        .withColumn(
            "arrival_delay_min",
            (
                F.unix_timestamp("actual_arrival_utc")
                - F.unix_timestamp("scheduled_arrival_utc")
            ) / 60.0
        )
        .withColumn(
            "is_completed",
            F.when(F.col("actual_arrival_utc").isNotNull(), 1).otherwise(0)
        )
        .withColumn(
            "is_departure_delayed",
            F.when(
                (F.col("actual_departure_utc").isNotNull()) &
                (F.col("scheduled_departure_utc").isNotNull()) &
                (F.col("actual_departure_utc") > F.col("scheduled_departure_utc")),
                1,
            ).otherwise(0)
        )
        .withColumn(
            "is_arrival_delayed",
            F.when(
                (F.col("actual_arrival_utc").isNotNull()) &
                (F.col("scheduled_arrival_utc").isNotNull()) &
                (F.col("actual_arrival_utc") > F.col("scheduled_arrival_utc")),
                1,
            ).otherwise(0)
        )
        .groupBy(
            "marketing_airline_id",
            "airline_name",
            "departure_airport_code",
            "departure_airport_name",
            "departure_country_code",
            "departure_country_name",
            "arrival_airport_code",
            "arrival_airport_name",
            "arrival_country_code",
            "arrival_country_name",
        )
        .agg(
            F.count("*").alias("total_flights"),
            F.sum("is_completed").alias("completed_flights_count"),
            F.round(
                F.avg(F.when(F.col("is_completed") == 1, F.col("departure_delay_min"))), 2
            ).alias("avg_departure_delay_min"),
            F.round(
                F.avg(F.when(F.col("is_completed") == 1, F.col("arrival_delay_min"))), 2
            ).alias("avg_arrival_delay_min"),
            F.sum("is_departure_delayed").alias("delayed_departures_count"),
            F.sum("is_arrival_delayed").alias("delayed_arrivals_count"),
        )
        .withColumn(
            "delayed_departures_pct",
            F.when(
                F.col("completed_flights_count") > 0,
                F.round(
                    F.col("delayed_departures_count") * 100.0 / F.col("completed_flights_count"), 2
                )
            )
        )
        .withColumn(
            "delayed_arrivals_pct",
            F.when(
                F.col("completed_flights_count") > 0,
                F.round(
                    F.col("delayed_arrivals_count") * 100.0 / F.col("completed_flights_count"), 2
                )
            )
        )
    )
