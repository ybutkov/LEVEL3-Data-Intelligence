from pyspark.sql.types import StructType, StructField, StringType, ArrayType


link_schema = StructType([
    StructField("@Href", StringType(), True),
    StructField("@Rel", StringType(), True),
])

meta_schema = StructType([
    StructField("@Version", StringType(), True),
    StructField("Link", ArrayType(link_schema), True),
    StructField("TotalCount", StringType(), True),
])

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
        StructField("Meta", meta_schema, True),
    ]), True),
])
