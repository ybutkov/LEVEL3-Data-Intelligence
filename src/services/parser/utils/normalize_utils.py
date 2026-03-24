from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, trim, upper, lower

REFERENCE_NORMALIZATION_MAP = {
    "ref_dim_country": ["country_code"],
    "ref_country_names_flat": ["country_code", "language_code"],

    "ref_dim_city": ["city_code", "country_code"],
    "ref_city_names_flat": ["city_code", "language_code"],
    "ref_city_airport_map": ["city_code", "airport_code"],

    "ref_dim_airport": ["airport_code", "city_code", "country_code"],
    "ref_airport_names_flat": ["airport_code", "language_code"],

    "ref_dim_airline": ["airline_id", "airline_id_icao"],
    "ref_airline_names_flat": ["airline_id", "language_code"],

    "ref_dim_aircraft": ["aircraft_code", "airline_equip_code"],
    "ref_aircraft_names_flat": ["aircraft_code", "language_code"],
}

    # opeational outputs
OPERATIONAL_NORMALIZATION_MAP = {
    "op_fact_flight_status": [
        "airline_id",
        "operating_airline_id",
        "dep_airport",
        "arr_airport",
        "flight_status_code",
        "dep_time_status_code",
        "arr_time_status_code",
        "aircraft_code",
    ]
}


def normalize_code_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    result_df = df

    for column in columns:
        if column in result_df.columns:
            result_df = result_df.withColumn(column, upper(trim(col(column)))) # Change to lower for test
    return result_df

def normalize_outputs(outputs: dict[str, DataFrame], normalization_map: dict[str, list[str]]):
    normalized_outputs = {}

    for table_name, df in outputs.items():
        columns_to_normalize = normalization_map.get(table_name, [])
        normalized_outputs[table_name] = normalize_code_columns(df, columns_to_normalize)

    return normalized_outputs
