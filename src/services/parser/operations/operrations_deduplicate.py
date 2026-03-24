from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, sha2, coalesce, lit


def add_flight_key_and_state_hash(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "flight_key",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("marketing_airline_id"), lit("")),
                    coalesce(col("marketing_flight_number"), lit("")),
                    coalesce(col("departure_airport_code"), lit("")),
                    coalesce(col("arrival_airport_code"), lit("")),
                    coalesce(col("scheduled_departure_utc").cast("string"), lit("")),
                ),
                256,
            ),
        )
        .withColumn(
            "state_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("actual_departure_utc").cast("string"), lit("")),
                    coalesce(col("estimated_departure_utc").cast("string"), lit("")),
                    coalesce(col("actual_arrival_utc").cast("string"), lit("")),
                    coalesce(col("estimated_arrival_utc").cast("string"), lit("")),
                    coalesce(col("departure_time_status_code"), lit("")),
                    coalesce(col("arrival_time_status_code"), lit("")),
                    coalesce(col("flight_status_code"), lit("")),
                    coalesce(col("departure_gate"), lit("")),
                    coalesce(col("arrival_gate"), lit("")),
                    coalesce(col("aircraft_code"), lit("")),
                    coalesce(col("aircraft_registration"), lit("")),
                ),
                256,
            ),
        )
    )


def filter_unchanged_flight(
    spark,
    new_df: DataFrame,
    target_table: str,
) -> DataFrame:

    new_df = add_flight_key_and_state_hash(new_df)

    if not spark.catalog.tableExists(target_table):
        return new_df

    existing_df = (
        spark.table(target_table)
        .select("flight_key", "state_hash")
        .dropDuplicates()
    )

    return new_df.join(
        existing_df,
        on=["flight_key", "state_hash"],
        how="left_anti",
    )
