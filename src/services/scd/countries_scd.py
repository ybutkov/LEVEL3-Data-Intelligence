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
@dp.expect_or_drop("valid_id", "country_code IS NOT NULL")
def countries_cleaned():
    return (
        dp.read_stream("lufthansa_level.bronze.countries_raw")
        .select(
            col("bronze_ingested_at").alias("sync_ts"),
            col("ingest_run_id"),
            from_json(col("raw_json"), country_resource_schema).alias("data")
        )
        .select(
            col("sync_ts"),
            col("ingest_run_id"),
            explode(col("data.CountryResource.Countries.Country")).alias("country")
        )
        .select(
            col("sync_ts"),
            col("ingest_run_id"),
            col("country.CountryCode").alias("country_code"),
            col("country.Names.Name").alias("names_array")
        )
    )


dp.create_streaming_table("ref_dim_country")

dp.apply_changes(
    target = "ref_dim_country",
    source = "countries_cleaned",
    keys = ["country_code"],
    sequence_by = col("sync_ts"),
    stored_as_scd_type = 1
)

@dp.view
def names_exploded():
    return (
        dp.read_stream("countries_cleaned")
        .select(
            col("sync_ts"),
            col("country_code"),
            col("ingest_run_id"),
            explode(col("names_array")).alias("n")
        )
        .select(
            col("sync_ts"),
            col("ingest_run_id"),
            col("country_code"),
            col("n.`@LanguageCode`").alias("language_code"),
            col("n.`$`").alias("country_name")
        )
    )

dp.create_streaming_table("ref_country_names_flat")

dp.apply_changes(
    target = "ref_country_names_flat",
    source = "names_exploded",
    keys = ["country_code", "language_code"],
    sequence_by = col("sync_ts"),
    stored_as_scd_type = 2
)
