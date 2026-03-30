from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json, upper, trim, lower, expr, udf
from pyspark.sql.types import StringType
import src.services.parsing_schemas as schemas
from src.services.scd.utils.rules import COUNTRY_RULES
import json


# UDF to normalize JSON: wrap single Name objects in array
@udf(returnType=StringType())
def normalize_country_json(raw_json_str):
    try:
        data = json.loads(raw_json_str)
        countries = data.get('CountryResource', {}).get('Countries', {}).get('Country', [])
        
        # Normalize each country's Name field to be an array
        for country in countries:
            names_obj = country.get('Names', {}).get('Name')
            # If Name is a dict (single object), wrap it in array
            if isinstance(names_obj, dict):
                country['Names']['Name'] = [names_obj]
        
        return json.dumps(data)
    except:
        return raw_json_str


COUNTRY_BRONZE_SOURCE = "lufthansa_level.bronze.countries_raw"
COUNTRY_META_FIELDS = ["source_file", "bronze_ingested_at", "ingest_run_id"]
entity_alias = "country"
code_field = "CountryCode"
code_alias = "country_code"
name_alias = "country_name"


@dp.table(name="silver_audit.err_country_invalid_json")
def country_invalid_json():
    return (
        dp.read_stream(COUNTRY_BRONZE_SOURCE)
        .withColumn("parsed", from_json(col("raw_json"), schemas.country_resource_schema))
        .filter(col("parsed").isNull())
        .select(
            *COUNTRY_META_FIELDS,
            col("raw_json")
        )
    )


@dp.view(name="exploded_country_entity")
def exploded_country_entity():
    return (
        dp.read_stream(COUNTRY_BRONZE_SOURCE)
        .withColumn("raw_json_normalized", normalize_country_json(col("raw_json")))
        .select(
            *[col(f) for f in COUNTRY_META_FIELDS],
            from_json(col("raw_json_normalized"), schemas.country_resource_schema).alias("data_json")
        )
        .filter(col("data_json").isNotNull())
        .select(
            *[col(f) for f in COUNTRY_META_FIELDS],
            explode(col("data_json.CountryResource.Countries.Country")).alias(entity_alias)
        )
        .select(
            "*",
            upper(trim(col(f"{entity_alias}.{code_field}"))).alias(code_alias)
        )
    )


@dp.view
def dim_country_rules_checked():
    df = dp.read_stream("exploded_country_entity")
    rules = COUNTRY_RULES["ref_dim_country"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    dim_quarantine_rules = "NOT({0})".format(combined_condition)
    return df.withColumn("is_dim_quarantined", expr(dim_quarantine_rules))


@dp.view
def dim_country_df():
    return (
        dp.read_stream("dim_country_rules_checked")
        .filter("is_dim_quarantined=false")
        .select(
            *COUNTRY_META_FIELDS,
            col(code_alias),
        )
    )


@dp.table(name="silver_audit.err_dim_country_quarantine")
def dim_country_quarantine():
    return (
        dp.read_stream("dim_country_rules_checked")
        .filter("is_dim_quarantined=true")
        .select(
            *COUNTRY_META_FIELDS,
            col(code_alias),
        )
    )


@dp.view
def country_names_flat_checked():
    df = dp.read_stream("exploded_country_entity")
    dim_rules = COUNTRY_RULES["ref_dim_country"]
    dim_combined = " AND ".join([f"({cond})" for cond in dim_rules.values()])
    
    df = df.filter(dim_combined)
    
    rules = COUNTRY_RULES["ref_country_names_flat"]
    combined_condition = " AND ".join([f"({cond})" for cond in rules.values()])
    quarantine_name_rules = "NOT({0})".format(combined_condition)
    
    df = df.withColumn(
        "names_array",
        col(f"{entity_alias}.Names.Name")
    ).select(
        *[col(f) for f in COUNTRY_META_FIELDS],
        col(code_alias),
        explode(col("names_array")).alias("n")
    ).select(
        *[col(f) for f in COUNTRY_META_FIELDS],
        col(code_alias),
        upper(trim(col("n.`@LanguageCode`"))).alias("language_code"),
        col("n.$").alias(name_alias),
    )
    
    return df.withColumn("is_names_quarantined", expr(quarantine_name_rules))


@dp.view
def country_names():
    return (
        dp.read_stream("country_names_flat_checked")
        .filter("is_names_quarantined=false")
        .select(
            *COUNTRY_META_FIELDS,
            col(code_alias),
            col("language_code"),
            trim(col(name_alias)).alias(name_alias),
        )
    )


@dp.table(name="silver_audit.err_country_names_quarantine")
def country_names_quarantine():
    return (
        dp.read_stream("country_names_flat_checked")
        .filter("is_names_quarantined=true")
        .select(
            *COUNTRY_META_FIELDS,
            col(code_alias),
            col("language_code"),
            trim(col(name_alias)).alias(name_alias),
        )
    )


dp.create_streaming_table("silver.ref_dim_country")
dp.create_auto_cdc_flow(
    target="silver.ref_dim_country",
    source="dim_country_df",
    keys=[code_alias],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=1
)


dp.create_streaming_table("silver.ref_country_names_flat")
dp.create_auto_cdc_flow(
    target="silver.ref_country_names_flat",
    source="country_names",
    keys=[code_alias, "language_code"],
    sequence_by=col("bronze_ingested_at"),
    stored_as_scd_type=2,
    track_history_column_list=[name_alias]
)
