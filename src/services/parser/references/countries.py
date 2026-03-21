from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode

from src.services.parser.utils.parser_utils import (
    build_parsed_df,
    build_table_name,
    split_valid_invalid,
    log_invalid_records,
    )
from src.config.endpoints import get_endpoint_config, EndpointKeys
import src.services.parsing_schemas as schemas


def transform_countries(parsed_df: DataFrame) -> DataFrame:
    return (
        parsed_df
        .select(explode(col("data_json.CountryResource.Countries.Country")).alias("country"))
        .select(
            col("country.CountryCode").alias("country_code"),
            explode(col("country.Names.Name")).alias("name")
        )
        .select(
            col("country_code"),
            col("name.@LanguageCode").alias("language_code"),
            col("name.$").alias("country_name")
        )
    )


def build_dim_country(country_names_flat_df: DataFrame) -> DataFrame:
    return (
        country_names_flat_df
        .filter(col("language_code") == "EN")
        .select(
            col("country_code"),
            col("country_name").alias("country_name_en")
        )
    )


def parse_countries(spark,cfg) -> None:
    # cfg = get_ConfigProperties()
    endpoint_cfg = get_endpoint_config(EndpointKeys.COUNTRIES)

    bronze_table = endpoint_cfg.build_bronze_table_name(cfg.storage)
    silver_country_names_table = build_table_name(cfg, cfg.storage.silver_schema, "country_names_flat")
    silver_dim_country_table = build_table_name(cfg, cfg.storage.silver_schema, "dim_country")
    silver_invalid_log_table = build_table_name(cfg, cfg.storage.silver_schema, "invalid_json_log")

    parsed_df = build_parsed_df(
        spark=spark,
        bronze_table=bronze_table,
        schema=schemas.country_resource_schema,
        raw_json_col="raw_json",
    )
    valid_df, invalid_df = split_valid_invalid(parsed_df)
    valid_df = valid_df.persist()

    log_invalid_records(
        invalid_df=invalid_df,
        log_table=silver_invalid_log_table,
        raw_json_col="raw_json",
        dataset_name=endpoint_cfg.bronze_table,
    )

    country_names_flat_df = transform_countries(valid_df).dropDuplicates(["country_code", "language_code"])
    dim_country_df = build_dim_country(country_names_flat_df)

    country_names_flat_df.write.mode("overwrite").saveAsTable(silver_country_names_table)
    dim_country_df.write.mode("overwrite").saveAsTable(silver_dim_country_table)
    valid_df.unpersist()

