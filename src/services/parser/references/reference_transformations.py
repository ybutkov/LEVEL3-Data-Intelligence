from pyspark.sql.functions import col

REFERENCE_TRANSFORMATION_MAP = {
    "ref_dim_country": [],
    "ref_country_names_flat": [],

    "ref_dim_city": [],
    "ref_city_names_flat": [],
    "ref_city_airport_map": [],

    # TODO: write 2 columns: raw and casted?
    "ref_dim_airport": [
        lambda df: df.withColumn("latitude", col("latitude").cast("double")),
        lambda df: df.withColumn("longitude", col("longitude").cast("double")),
    ],
    "ref_airport_names_flat": [],

    "ref_dim_airline": [],
    "ref_airline_names_flat": [],

    "ref_dim_aircraft": [],
    "ref_aircraft_names_flat": [],
}
