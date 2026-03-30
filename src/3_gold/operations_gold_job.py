import sys, os, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args,_ = parser.parse_known_args()
if args.root_path and args.root_path not in sys.path:
    sys.path.insert(0, args.root_path)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.app.logger import get_logger
from src.app.init_app import init_app
from src.config.config_properties import get_ConfigProperties
from pyspark.sql import SparkSession

logger = get_logger(__name__)


def build_table_name(cfg, schema_name: str, table_name: str) -> str:
    return f"{cfg.storage.catalog}.{schema_name}.{table_name}"


def pick_name_by_lang(df, key_col: str, name_col: str, lang: str = "EN"):
    ranked = (
        df
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy(key_col).orderBy(
                    F.when(F.col("language_code") == lang, F.lit(1)).otherwise(F.lit(2)),
                    F.col(name_col).asc_nulls_last(),
                )
            ),
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    return ranked


def run_operations_gold(spark, cfg):
    logger.info("Start operations gold job")

    silver_schema = cfg.storage.silver_schema
    gold_schema = cfg.storage.gold_schema

    silver_flight_status = build_table_name(cfg, silver_schema, "op_fact_flight_status")
    ref_dim_airport = build_table_name(cfg, silver_schema, "ref_dim_airport")
    ref_airport_names = build_table_name(cfg, silver_schema, "ref_airport_names_flat")
    ref_airline_names = build_table_name(cfg, silver_schema, "ref_airline_names_flat")
    ref_country_names = build_table_name(cfg, silver_schema, "ref_country_names_flat")

    gold_latest_table = build_table_name(cfg, gold_schema, "gold_flight_status_latest")
    gold_airline_route_table = build_table_name(cfg, gold_schema, "gold_airline_route_performance")

    flights_df = spark.table(silver_flight_status)

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

    airline_names_df = (
        pick_name_by_lang(
            spark.table(ref_airline_names),
            key_col="airline_id",
            name_col="airline_name",
            lang="EN",
        )
        .select(
            F.col("airline_id").alias("ref_marketing_airline_id"),
            F.col("airline_name"),
        )
    )

    airport_names_df = pick_name_by_lang(
        spark.table(ref_airport_names),
        key_col="airport_code",
        name_col="airport_name",
        lang="EN",
    ).select("airport_code", "airport_name")

    airport_dim_df = (
        spark.table(ref_dim_airport)
        .select(
            F.col("airport_code").alias("dim_airport_code"),
            F.col("country_code").alias("dim_country_code"),
        )
    )

    country_names_df = (
        pick_name_by_lang(
            spark.table(ref_country_names),
            key_col="country_code",
            name_col="country_name",
            lang="EN",
        )
        .select(
            F.col("country_code").alias("dim_country_code"),
            F.col("country_name"),
        )
    )

    enriched_latest_df = (
        latest_df

        # airline name
        .join(
            airline_names_df,
            latest_df.marketing_airline_id == airline_names_df.ref_marketing_airline_id,
            "left",
        )
        .drop("ref_marketing_airline_id")

        # departure airport name
        .join(
            airport_names_df.alias("dep_airport_names"),
            F.col("departure_airport_code") == F.col("dep_airport_names.airport_code"),
            "left",
        )
        .withColumnRenamed("airport_name", "departure_airport_name")
        .drop(F.col("dep_airport_names.airport_code"))

        # arrival airport name
        .join(
            airport_names_df.alias("arr_airport_names"),
            F.col("arrival_airport_code") == F.col("arr_airport_names.airport_code"),
            "left",
        )
        .withColumnRenamed("airport_name", "arrival_airport_name")
        .drop(F.col("arr_airport_names.airport_code"))

        # departure country code
        .join(
            airport_dim_df.alias("dep_airport_dim"),
            F.col("departure_airport_code") == F.col("dep_airport_dim.dim_airport_code"),
            "left",
        )
        .withColumnRenamed("dim_country_code", "departure_country_code")
        .drop(F.col("dep_airport_dim.dim_airport_code"))

        # arrival country code
        .join(
            airport_dim_df.alias("arr_airport_dim"),
            F.col("arrival_airport_code") == F.col("arr_airport_dim.dim_airport_code"),
            "left",
        )
        .withColumnRenamed("dim_country_code", "arrival_country_code")
        .drop(F.col("arr_airport_dim.dim_airport_code"))

        # departure country name
        .join(
            country_names_df.alias("dep_country_names"),
            F.col("departure_country_code") == F.col("dep_country_names.dim_country_code"),
            "left",
        )
        .withColumnRenamed("country_name", "departure_country_name")
        .drop(F.col("dep_country_names.dim_country_code"))

        # arrival country name
        .join(
            country_names_df.alias("arr_country_names"),
            F.col("arrival_country_code") == F.col("arr_country_names.dim_country_code"),
            "left",
        )
        .withColumnRenamed("country_name", "arrival_country_name")
        .drop(F.col("arr_country_names.dim_country_code"))
    )

    enriched_latest_df.write.mode("overwrite").saveAsTable(gold_latest_table)
    logger.info(f"Saved latest gold table: {gold_latest_table}")

    airline_route_df = (
        enriched_latest_df
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
                F.avg(F.when(F.col("is_completed") == 1, F.col("departure_delay_min"))),2).alias("avg_departure_delay_min"),
            F.round(
                F.avg(F.when(F.col("is_completed") == 1, F.col("arrival_delay_min"))),2).alias("avg_arrival_delay_min"),
            F.sum("is_departure_delayed").alias("delayed_departures_count"),
            F.sum("is_arrival_delayed").alias("delayed_arrivals_count"),
        )
        .withColumn(
            "delayed_departures_pct",
            F.when(
                F.col("completed_flights_count") > 0,
                F.round(F.col("delayed_departures_count") * 100.0 / F.col("completed_flights_count"), 2))
            )
        .withColumn(
            "delayed_arrivals_pct",
            F.when(
                F.col("completed_flights_count") > 0,
                F.round(F.col("delayed_arrivals_count") * 100.0 / F.col("completed_flights_count"), 2))
        )
    )

    airline_route_df.write.mode("overwrite").saveAsTable(gold_airline_route_table)
    logger.info(f"Saved airline route gold table: {gold_airline_route_table}")

    logger.info("Finish operations gold job")


if __name__ == "__main__":

    init_app()
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    cfg = get_ConfigProperties()

    run_operations_gold(spark, cfg)
