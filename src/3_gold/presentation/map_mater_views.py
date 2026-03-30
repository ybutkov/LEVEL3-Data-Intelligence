import sys
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Set up sys.path for DLT pipeline context
root_path = spark.conf.get("root_path")
if root_path and root_path not in sys.path:
    sys.path.insert(0, root_path)

from src.config.config_properties import get_ConfigProperties
from src.util.tables_utils import build_table_name

# Supported languages for names
SUPPORTED_LANGUAGES = ("EN", "DE", "FR")
DACH_COUNTRIES = ("DE", "AT", "CH")  # Germany, Austria, Switzerland
@dp.materialized_view(
    name="gold_viz_airport_map_points",
    comment="Airport map visualization - total flight volume and delay metrics by airport. Shows both departures and arrivals aggregated at airport level."
)
def gold_viz_airport_map_points():
    """
    Airport-level aggregation for map visualization.
    Each airport gets one marker showing total activity and delays.
    """
    cfg = get_ConfigProperties()    
    traffic = spark.read.table(build_table_name(cfg, cfg.storage.gold_schema, "gold_airport_traffic")).alias("traffic")
    airports = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_dim_airport")).alias("airports")
    air_names = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_airport_names_flat")).filter(F.col("language_code").isin(*SUPPORTED_LANGUAGES)).alias("air_names")
    country_names = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_country_names_flat")).filter(F.col("language_code").isin(*SUPPORTED_LANGUAGES)).alias("country_names")

    # Aggregate all metrics at airport level (both departures and arrivals)
    airport_agg = (
        traffic
        .filter(F.col("country_code").isin(*DACH_COUNTRIES))  # Only DACH countries
        .groupBy("airport_code", "country_code")
        .agg(
            F.sum(F.col("total_departures")).alias("total_departures"),
            F.sum(F.col("total_movements")).alias("total_movements"),
            F.sum(F.col("total_completed")).alias("total_completed"),
            F.sum(F.col("total_delayed")).alias("total_delayed"),
        )
        .withColumn("total_delayed", F.coalesce(F.col("total_delayed"), F.lit(0)))
        .withColumn("total_movements", F.coalesce(F.col("total_movements"), F.lit(0)))
        .withColumn(
            "delay_rate_pct",
            F.round(
                F.when(F.col("total_movements") > 0, (F.col("total_delayed") / F.col("total_movements")) * 100).otherwise(0),
                2
            )
        )
    )

    return (
        airport_agg.alias("agg")
        .join(airports, F.col("agg.airport_code") == F.col("airports.airport_code"), "left")
        .join(air_names, F.col("agg.airport_code") == F.col("air_names.airport_code"), "left")
        .join(
            country_names,
            (F.col("agg.country_code") == F.col("country_names.country_code")) &
            (F.col("air_names.language_code") == F.col("country_names.language_code")),
            "left"
        )
        .select(
            F.col("agg.airport_code").alias("airport_code"),
            F.col("air_names.language_code").alias("language_code"),
            F.col("air_names.airport_name").alias("airport_name"),
            F.col("agg.country_code").alias("country_code"),
            F.col("country_names.country_name").alias("country_name"),
            F.expr("ST_Y(airports.coordinates)").alias("latitude"),
            F.expr("ST_X(airports.coordinates)").alias("longitude"),
            F.col("agg.total_movements").alias("total_movements"),
            F.col("agg.total_departures").alias("total_departures"),
            F.col("agg.total_completed").alias("total_completed"),
            F.col("agg.total_delayed").alias("total_delayed"),
            F.col("agg.delay_rate_pct").alias("delay_rate_pct"),
            F.current_timestamp().alias("processed_at"),
        )
    )


@dp.materialized_view(
    name="gold_viz_route_map_lines",
    comment="Route map visualization - flight flows between airports with delay metrics per route and airline."
)
def gold_viz_route_map_lines():
    """
    Route-level aggregation for map visualization.
    Shows flows between airports with delay and volume information.
    """
    cfg = get_ConfigProperties()
    route_perf = spark.read.table(build_table_name(cfg, cfg.storage.gold_schema, "gold_airline_route_performance")).alias("route_perf")
    dep_air = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_dim_airport")).alias("dep_air")
    arr_air = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_dim_airport")).alias("arr_air")
    
    # Filter names to only supported languages
    dep_airnames = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_airport_names_flat")).filter(F.col("language_code").isin(*SUPPORTED_LANGUAGES)).alias("dep_airnames")
    arr_airnames = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_airport_names_flat")).filter(F.col("language_code").isin(*SUPPORTED_LANGUAGES)).alias("arr_airnames")
    dep_country_names = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_country_names_flat")).filter(F.col("language_code").isin(*SUPPORTED_LANGUAGES)).alias("dep_country_names")
    arr_country_names = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_country_names_flat")).filter(F.col("language_code").isin(*SUPPORTED_LANGUAGES)).alias("arr_country_names")
    air_names = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_airline_names_flat")).filter(F.col("language_code").isin(*SUPPORTED_LANGUAGES)).alias("air_names")

    return (
        route_perf
        .filter(
            (F.col("departure_country_code").isin(*DACH_COUNTRIES)) |
            (F.col("arrival_country_code").isin(*DACH_COUNTRIES))
        )  # Only routes involving DACH airports (departure or arrival)
        .withColumn("avg_departure_delay_min", F.coalesce(F.col("avg_departure_delay_min"), F.lit(0)))
        .withColumn("avg_arrival_delay_min", F.coalesce(F.col("avg_arrival_delay_min"), F.lit(0)))
        .withColumn("delayed_departures_pct", F.coalesce(F.col("delayed_departures_pct"), F.lit(0)))
        .withColumn("delayed_arrivals_pct", F.coalesce(F.col("delayed_arrivals_pct"), F.lit(0)))
        .withColumn("delayed_departures_pct", F.round(F.col("delayed_departures_pct"), 2))
        .withColumn("delayed_arrivals_pct", F.round(F.col("delayed_arrivals_pct"), 2))
        .join(
            dep_air,
            F.col("route_perf.departure_airport_code") == F.col("dep_air.airport_code"),
            "left"
        )
        .join(
            arr_air,
            F.col("route_perf.arrival_airport_code") == F.col("arr_air.airport_code"),
            "left"
        )
        .join(
            dep_airnames,
            F.col("route_perf.departure_airport_code") == F.col("dep_airnames.airport_code"),
            "left"
        )
        .join(
            arr_airnames,
            (F.col("route_perf.arrival_airport_code") == F.col("arr_airnames.airport_code")) &
            (F.col("dep_airnames.language_code") == F.col("arr_airnames.language_code")),
            "left"
        )
        .join(
            dep_country_names,
            (F.col("route_perf.departure_country_code") == F.col("dep_country_names.country_code")) &
            (F.col("dep_airnames.language_code") == F.col("dep_country_names.language_code")),
            "left"
        )
        .join(
            arr_country_names,
            (F.col("route_perf.arrival_country_code") == F.col("arr_country_names.country_code")) &
            (F.col("dep_airnames.language_code") == F.col("arr_country_names.language_code")),
            "left"
        )
        .join(
            air_names,
            (F.col("route_perf.marketing_airline_id") == F.col("air_names.airline_id")) &
            (F.col("dep_airnames.language_code") == F.col("air_names.language_code")),
            "left"
        )
        .select(
            F.col("route_perf.departure_airport_code").alias("departure_airport_code"),
            F.col("route_perf.arrival_airport_code").alias("arrival_airport_code"),
            F.col("dep_airnames.language_code").alias("language_code"),
            F.col("route_perf.marketing_airline_id").alias("marketing_airline_id"),
            F.col("air_names.airline_name").alias("airline_name"),
            F.col("dep_airnames.airport_name").alias("departure_airport_name"),
            F.col("route_perf.departure_country_code").alias("departure_country_code"),
            F.col("dep_country_names.country_name").alias("departure_country_name"),
            F.col("arr_airnames.airport_name").alias("arrival_airport_name"),
            F.col("route_perf.arrival_country_code").alias("arrival_country_code"),
            F.col("arr_country_names.country_name").alias("arrival_country_name"),
            F.expr("ST_Y(dep_air.coordinates)").alias("departure_lat"),
            F.expr("ST_X(dep_air.coordinates)").alias("departure_lon"),
            F.expr("ST_Y(arr_air.coordinates)").alias("arrival_lat"),
            F.expr("ST_X(arr_air.coordinates)").alias("arrival_lon"),
            F.col("route_perf.total_flights").alias("total_flights"),
            F.col("avg_departure_delay_min").alias("avg_departure_delay_min"),
            F.col("avg_arrival_delay_min").alias("avg_arrival_delay_min"),
            F.col("delayed_departures_pct").alias("delayed_departures_pct"),
            F.col("delayed_arrivals_pct").alias("delayed_arrivals_pct"),
            F.when(
                F.col("route_perf.departure_country_code") == F.col("route_perf.arrival_country_code"),
                F.lit(True)
            ).otherwise(F.lit(False)).alias("is_domestic"),
            F.current_timestamp().alias("processed_at"),
        )
    )
