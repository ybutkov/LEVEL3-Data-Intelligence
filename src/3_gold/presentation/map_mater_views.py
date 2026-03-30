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
SUPPORTED_LANGUAGES = ("EN", "DE")


# def build_table_name(cfg, schema_name: str, table_name: str) -> str:
#     return f"{cfg.storage.catalog}.{schema_name}.{table_name}"


@dp.materialized_view(
    name="gold_viz_airport_map_points",
    comment="Airport-level dataset for map points with translated labels, coordinates, and traffic/delay metrics"
)
def gold_viz_airport_map_points():
    cfg = get_ConfigProperties()    
    traffic = spark.read.table(build_table_name(cfg, cfg.storage.gold_schema, "gold_airport_traffic")).alias("traffic")
    airports = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_dim_airport")).alias("airports")
    air_names = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_airport_names_flat")).filter(F.col("language_code").isin(*SUPPORTED_LANGUAGES)).alias("air_names")
    country_names = spark.read.table(build_table_name(cfg, cfg.storage.silver_schema, "ref_country_names_flat")).filter(F.col("language_code").isin(*SUPPORTED_LANGUAGES)).alias("country_names")

    return (
        traffic
        .filter(F.col("traffic.movement_type") == "DEPARTURE")
        .join(airports, F.col("traffic.airport_code") == F.col("airports.airport_code"), "left")
        .join(air_names, F.col("traffic.airport_code") == F.col("air_names.airport_code"), "left")
        .join(
            country_names,
            (F.col("traffic.country_code") == F.col("country_names.country_code")) &
            (F.col("air_names.language_code") == F.col("country_names.language_code")),
            "left"
        )
        .select(
            F.col("traffic.airport_code").alias("airport_code"),
            F.col("air_names.language_code").alias("language_code"),
            F.col("air_names.airport_name").alias("airport_name"),
            F.col("traffic.country_code").alias("country_code"),
            F.col("country_names.country_name").alias("country_name"),
            F.expr("ST_Y(airports.coordinates)").alias("latitude"),
            F.expr("ST_X(airports.coordinates)").alias("longitude"),
            F.col("traffic.total_departures").alias("total_departures"),
            F.col("traffic.completed_departures").alias("completed_departures"),
            F.col("traffic.delayed_departures").alias("delayed_departures"),
            F.col("traffic.total_movements").alias("total_movements"),
            F.col("traffic.total_completed").alias("total_completed"),
            F.col("traffic.total_delayed").alias("total_delayed"),
            F.col("traffic.avg_delay_min").alias("avg_delay_min"),
            F.col("traffic.delay_rate_pct").alias("delay_rate_pct"),
            F.current_timestamp().alias("processed_at"),
        )
    )

@dp.materialized_view(
    name="gold_viz_route_map_lines",
    comment="Route-level dataset for map lines with translated labels, coordinates, airline names, and route delay metrics"
)
def gold_viz_route_map_lines():
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
            F.col("route_perf.completed_flights_count").alias("completed_flights_count"),
            F.col("route_perf.avg_departure_delay_min").alias("avg_departure_delay_min"),
            F.col("route_perf.avg_arrival_delay_min").alias("avg_arrival_delay_min"),
            F.col("route_perf.delayed_departures_count").alias("delayed_departures_count"),
            F.col("route_perf.delayed_arrivals_count").alias("delayed_arrivals_count"),
            F.col("route_perf.delayed_departures_pct").alias("delayed_departures_pct"),
            F.col("route_perf.delayed_arrivals_pct").alias("delayed_arrivals_pct"),
            F.when(
                F.col("route_perf.departure_country_code") == F.col("route_perf.arrival_country_code"),
                F.lit(True)
            ).otherwise(F.lit(False)).alias("is_domestic"),
            F.current_timestamp().alias("processed_at"),
        )
    )
