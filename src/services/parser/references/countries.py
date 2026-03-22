from src.app.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode

from src.services.parser.utils.parser_utils import (
    build_parsed_df,
    build_table_name,
    split_valid_invalid,
    log_invalid_records,
    add_silver_metadata,
    )
from src.config.endpoints import get_endpoint_config, EndpointKeys
import src.services.parsing_schemas as schemas


logger = get_logger(__name__)

def transform_countries(parsed_df: DataFrame) -> DataFrame:
    logger.info("Start transform_countries")
    result_df = (
        parsed_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col("data_json.CountryResource.Countries.Country")).alias("country")
            )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("country.CountryCode").alias("country_code"),
            explode(col("country.Names.Name")).alias("name")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("country_code"),
            col("name.@LanguageCode").alias("language_code"),
            col("name.$").alias("country_name")
        )
    )

    logger.info("Finish transform_countries")
    return result_df


def build_ref_dim_country(country_names_flat_df: DataFrame) -> DataFrame:
    logger.info("Start build_ref_dim_country")

    result_df = (
        country_names_flat_df
        .filter(col("language_code") == "EN")
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col("country_code"),
            col("country_name").alias("country_name_en")
        )
    )

    logger.info("Finish build_ref_dim_country")
    return result_df


def parse_countries(spark,cfg) -> None:
    # cfg = get_ConfigProperties()
    endpoint_cfg = get_endpoint_config(EndpointKeys.COUNTRIES)

    bronze_table = endpoint_cfg.build_bronze_table_name(cfg.storage)
    silver_country_names_table = build_table_name(cfg, cfg.storage.silver_schema, "ref_country_names_flat")
    silver_dim_country_table = build_table_name(cfg, cfg.storage.silver_schema, "ref_dim_country")
    silver_invalid_log_table = build_table_name(cfg, cfg.storage.silver_schema, "audit_invalid_json_log")

    logger.info("Start parse_countries")
    logger.info(f"Source bronze table: {bronze_table}")
    logger.info(f"Target flat table: {silver_country_names_table}")
    logger.info(f"Target dim table: {silver_dim_country_table}")
    logger.info(f"Target invalid log table: {silver_invalid_log_table}")

    try:
        parsed_df = build_parsed_df(
            spark=spark,
            bronze_table=bronze_table,
            schema=schemas.country_resource_schema,
            raw_json_col="raw_json",
        )
        # parsed_count = parsed_df.count()
        # logger.info(f"Parsed dataframe rows: {parsed_count}")

        valid_df, invalid_df = split_valid_invalid(parsed_df)
        # valid_df = valid_df.persist()

        log_invalid_records(
            invalid_df=invalid_df,
            log_table=silver_invalid_log_table,
            raw_json_col="raw_json",
            dataset_name=endpoint_cfg.bronze_table,
        )

        country_names_flat_df = transform_countries(valid_df).dropDuplicates(["country_code", "language_code"])
        country_names_flat_df = add_silver_metadata(country_names_flat_df)

        ref_dim_country_df = build_ref_dim_country(country_names_flat_df)
        ref_dim_country_df = add_silver_metadata(ref_dim_country_df)

        logger.info(f"Writing table: {silver_country_names_table}")
        country_names_flat_df.write.mode("overwrite").saveAsTable(silver_country_names_table)

        logger.info(f"Writing table: {silver_dim_country_table}")
        ref_dim_country_df.write.mode("overwrite").saveAsTable(silver_dim_country_table)

        logger.info("Countries silver tables written successfully")
    except Exception as e:
        logger.exception(f"parse_countries failed: {e}")
        raise

    # valid_df.unpersist()

    logger.info("Finish parse_countries")
