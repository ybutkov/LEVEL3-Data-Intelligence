from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, lit


def build_parsed_df(
    spark,
    bronze_table: str,
    schema,
    raw_json_col: str = "raw_json",
) -> DataFrame:
    
    raw_df = spark.table(bronze_table)

    return raw_df.withColumn(
        "data_json",
        from_json(col(raw_json_col), schema)
    )


def split_valid_invalid(
    parsed_df: DataFrame,
    parsed_col: str = "data_json",
) -> tuple[DataFrame, DataFrame]:
    
    valid_df = parsed_df.filter(col(parsed_col).isNotNull())
    invalid_df = parsed_df.filter(col(parsed_col).isNull())

    return valid_df, invalid_df


def log_invalid_records(
    invalid_df: DataFrame,
    log_table: str,
    raw_json_col: str = "raw_json",
    dataset_name: str | None = None,
) -> None:
    
    log_df = invalid_df.select(
        col(raw_json_col).alias("raw_json")
    )

    if dataset_name is not None:
        log_df = log_df.withColumn("dataset_name", lit(dataset_name))

    log_df.write.mode("append").saveAsTable(log_table)


def build_table_name(cfg, layer: str, name: str):
    return f"{cfg.storage.catalog}.{layer}.{name}"
