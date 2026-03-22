from src.app.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, lit, current_timestamp


logger = get_logger(__name__)

def build_parsed_df(
    spark,
    bronze_table: str,
    schema,
    raw_json_col: str = "raw_json",
) -> DataFrame:
    
    logger.info(f"Start build_parsed_df: bronze_table={bronze_table}, raw_json_col={raw_json_col}")
    raw_df = spark.table(bronze_table)

    parsed_df = raw_df.withColumn(
        "data_json",
        from_json(col(raw_json_col), schema)
    )

    logger.info(f"Finish build_parsed_df: bronze_table={bronze_table}")
    return parsed_df


def split_valid_invalid(
    parsed_df: DataFrame,
    parsed_col: str = "data_json",
) -> tuple[DataFrame, DataFrame]:
    
    logger.info(f"Start split_valid_invalid: parsed_col={parsed_col}")
    valid_df = parsed_df.filter(col(parsed_col).isNotNull())
    invalid_df = parsed_df.filter(col(parsed_col).isNull())

    logger.info("Finish split_valid_invalid")
    return valid_df, invalid_df


def log_invalid_records(
    invalid_df: DataFrame,
    log_table: str,
    raw_json_col: str = "raw_json",
    dataset_name: str | None = None,
) -> None:
    
    log_df = invalid_df.select(
        col(raw_json_col).alias("raw_json"),
        col("source_file"),
        col("bronze_ingested_at"),
    )

    if dataset_name is not None:
        log_df = log_df.withColumn("dataset_name", lit(dataset_name))

    invalid_count = log_df.count()
    if invalid_count > 0:
        log_df = log_df.withColumn("logged_at", current_timestamp())
        log_df.write.mode("append").saveAsTable(log_table)
        logger.warning(
            f"Invalid rows written to {log_table}, dataset_name={dataset_name}: {invalid_count}"
        )

def add_silver_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("processed_at", current_timestamp())


def build_table_name(cfg, layer: str, name: str):
    return f"{cfg.storage.catalog}.{layer}.{name}"
