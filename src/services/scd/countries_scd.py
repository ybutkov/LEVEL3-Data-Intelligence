from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, expr, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


# Schema definitions matching parser solution
name_schema = StructType([
    StructField("@LanguageCode", StringType(), True),
    StructField("$", StringType(), True)
])

country_schema = StructType([
    StructField("CountryCode", StringType(), True),
    StructField("Names", StructType([
        StructField("Name", ArrayType(name_schema), True)
    ]), True),
])

country_resource_schema = StructType([
    StructField("CountryResource", StructType([
        StructField("Countries", StructType([
            StructField("Country", ArrayType(country_schema), True)
        ]), True),
    ]), True),
])


@dp.view
def countries_raw_cdc():
    return spark.readStream.option("readChangeData", "true").table("lufthansa_level.bronze.countries_raw")


@dp.view
def countries_parsed():
    countries_cdf = spark.readStream.table("countries_raw_cdc")
    
    parsed_df = (
        countries_cdf
        .select(
            col("_change_type"),
            col("_commit_version"),
            col("source_file"),
            col("ingest_run_id"),
            from_json(col("raw_json"), country_resource_schema).alias("data_json")
        )
        .select(
            col("_change_type"),
            col("_commit_version"),
            col("source_file"),
            col("ingest_run_id"),
            explode(col("data_json.CountryResource.Countries.Country")).alias("country")
        )
    )
    
    return parsed_df


@dp.view
def countries_dim_data():
    parsed = spark.readStream.table("countries_parsed")
    return (
        parsed
        .select(
            col("source_file"),
            col("ingest_run_id"),
            col("country.CountryCode").alias("country_code"),
        )
        .dropDuplicates(["country_code"])
    )


@dp.table
def ref_dim_country():
    return spark.readStream.table("countries_dim_data")


@dp.view
def country_names_data():
    parsed = spark.readStream.table("countries_parsed")
    
    return (
        parsed
        .select(
            col("source_file"),
            col("ingest_run_id"),
            col("country.CountryCode").alias("country_code"),
            explode(col("country.Names.Name")).alias("name")
        )
        .select(
            col("source_file"),
            col("ingest_run_id"),
            col("country_code"),
            col("name.`@LanguageCode`").alias("language_code"),
            col("name.`$`").alias("country_name"),
        )
        .dropDuplicates(["country_code", "language_code"])
    )


@dp.table
def ref_country_names_flat():
    return spark.readStream.table("country_names_data")

