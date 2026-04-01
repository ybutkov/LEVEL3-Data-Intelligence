import sys
from pyspark import pipelines as dp
from pyspark.sql import functions as F

root_path = spark.conf.get("root_path")
if root_path and root_path not in sys.path:
    sys.path.insert(0, root_path)

from src.util.tables_utils import build_full_table_name

from pyspark.sql import functions as F

# Configuration
CATALOG = spark.conf.get("catalog")
SILVER_SCHEMA = spark.conf.get("silver_schema")
GOLD_SCHEMA = spark.conf.get("gold_schema")

# Constants
MIN_DELAY_MINUTES = 10 
MAX_REASONABLE_DELAY = 60*3 

# Manual mapping for airports missing from the silver.ref_dim_airport table
AIRPORT_FIX = {
    'SFO': 'US', 'TLV': 'IL', 'RDU': 'US', 'TFU': 'CN', 
    'NBJ': 'TH', 'VAR': 'BG', 'OLB': 'IT', 'COV': 'GB', 
    'KLX': 'GR', 'SCQ': 'ES'
}

def build_full_table_name(catalog, schema, table):
    return f"{catalog}.{schema}.{table}"

@dp.materialized_view(
    name=build_full_table_name(CATALOG, GOLD_SCHEMA, "gold_flight_metrics"),
    comment="Flight metrics with resolved ambiguous columns and manual airport mapping"
)
def gold_flight_metrics():
    # Prepare mapping expression for Spark
    mapping_expr = F.create_map([F.lit(x) for x in sum(AIRPORT_FIX.items(), ())])

    # Load Silver tables with aliases to avoid AmbiguousReference errors
    identity_df = spark.table(build_full_table_name(CATALOG, SILVER_SCHEMA, "fact_flight_identity")).alias("id")
    status_df = spark.table(build_full_table_name(CATALOG, SILVER_SCHEMA, "fact_flight_status")).alias("st")
    airport_dim = spark.table(build_full_table_name(CATALOG, SILVER_SCHEMA, "ref_dim_airport")).select(
        F.col("airport_code"), F.col("country_code").alias("apt_country")
    )
    
    # Initial Join - Resolution of ambiguous columns happens here using aliases
    flights_base = (
        identity_df.join(status_df, on="flight_key", how="left")
        .withColumn("departure_day_name", F.date_format(F.col("id.scheduled_departure_utc"), "EEEE"))
        .withColumn("day_num", F.dayofweek(F.col("id.scheduled_departure_utc")))
    )
    
    # Join with Airport Dimension
    enriched = (
        flights_base
        .join(F.broadcast(airport_dim.alias("dep_dim")), F.col("id.departure_airport_code") == F.col("dep_dim.airport_code"), "left")
        .join(F.broadcast(airport_dim.alias("arr_dim")), F.col("id.arrival_airport_code") == F.col("arr_dim.airport_code"), "left")
    )
    
    return (
        enriched
        # Fill missing country codes from AIRPORT_FIX if dim table returns NULL
        .withColumn("departure_country_code", 
            F.coalesce(F.col("dep_dim.apt_country"), mapping_expr[F.col("id.departure_airport_code")]))
        .withColumn("arrival_country_code", 
            F.coalesce(F.col("arr_dim.apt_country"), mapping_expr[F.col("id.arrival_airport_code")]))
        
        # Flight Status Logic
        .withColumn("is_completed", 
            F.when(F.col("st.actual_arrival_utc").isNotNull() | F.col("st.flight_status_code").isin("LD", "ON"), 1).otherwise(0))
        .withColumn("is_cancelled", 
            F.when(F.col("st.flight_status_code").isin("CA", "CD", "NA"), 1).otherwise(0))
        
        # Delay Calculation (Referencing id.scheduled_departure_utc specifically)
        .withColumn("departure_delay_min", 
            F.least(F.lit(MAX_REASONABLE_DELAY), 
                F.greatest(F.lit(0), (F.unix_timestamp("st.actual_departure_utc") - F.unix_timestamp("id.scheduled_departure_utc")) / 60.0)))
        
        # Delay Flag for Completed Flights
        .withColumn("is_dep_delayed", 
            F.when((F.col("departure_delay_min") >= MIN_DELAY_MINUTES) & (F.col("is_completed") == 1), 1).otherwise(0))
        
        # Select final columns to keep the table clean
        .select(
            F.col("id.flight_key"), 
            F.col("id.marketing_flight_number"),
            F.col("id.departure_airport_code"), 
            F.col("id.arrival_airport_code"),
            "departure_country_code", 
            "arrival_country_code",
            "departure_day_name", 
            "day_num", 
            "is_completed", 
            "is_cancelled", 
            "is_dep_delayed", 
            "departure_delay_min",
            F.col("st.flight_status_code")
        )
    )

@dp.materialized_view(
    name=build_full_table_name(CATALOG, GOLD_SCHEMA, "gold_airport_traffic"),
    comment="Aggregated airport traffic and punctuality metrics"
)
def gold_airport_traffic():
    metrics = spark.table(build_full_table_name(CATALOG, GOLD_SCHEMA, "gold_flight_metrics"))
    
    def get_traffic_by_mode(df, mode):
        is_dep = (mode == "DEPARTURE")
        pfx = "departure" if is_dep else "arrival"
        
        return (
            df.filter(F.col(f"{pfx}_airport_code").isNotNull())
            .groupBy(
                F.col(f"{pfx}_airport_code").alias("airport_code"),
                F.col(f"{pfx}_country_code").alias("country_code"),
                F.col("departure_day_name").alias("day_name"),
                F.col("day_num")
            )
            .agg(
                F.count("*").alias("total_events"),
                F.sum("is_completed").alias("total_completed"),
                F.sum("is_cancelled").alias("total_cancelled"),
                F.sum("is_dep_delayed" if is_dep else F.lit(0)).alias("total_delayed"),
                F.sum(F.when(F.col("is_completed") == 1, F.col("departure_delay_min") if is_dep else F.lit(0))).alias("sum_delay_min")
            )
            .withColumn("event_type", F.lit(mode))
        )

    dept_df = get_traffic_by_mode(metrics, "DEPARTURE")
    arr_df = get_traffic_by_mode(metrics, "ARRIVAL")
    
    combined = dept_df.unionByName(arr_df)
    
    # Global totals per airport/day to avoid duplicate counting in visuals
    overall_totals = (
        combined.groupBy("airport_code", "country_code", "day_name")
        .agg(F.sum("total_events").alias("total_events_overall"))
    )
    
    return (
        combined.join(overall_totals, on=["airport_code", "country_code", "day_name"], how="left")
        .withColumn("delay_rate_pct", 
            F.round(F.col("total_delayed") * 100.0 / F.when(F.col("total_completed") > 0, F.col("total_completed")), 2))
        .withColumn("avg_delay_min", 
            F.round(F.col("sum_delay_min") / F.when(F.col("total_delayed") > 0, F.col("total_delayed")), 2))
    )