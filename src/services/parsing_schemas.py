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

city_schema = StructType([
    StructField("CityCode", StringType(), True),
    StructField("CountryCode", StringType(), True),
    StructField("UtcOffset", StringType(), True),
    StructField("TimeZoneId", StringType(), True),
    StructField("Names", StructType([
        StructField("Name", ArrayType(name_schema), True)
    ]), True),
    StructField("Airports", StructType([
        StructField("AirportCode", ArrayType(StringType()), True)
    ]), True),
])

city_resource_schema = StructType([
    StructField("CityResource", StructType([
        StructField("Cities", StructType([
            StructField("City", ArrayType(city_schema), True)
        ]), True),
        StructField("Meta", meta_schema, True),
    ]), True),
])

airport_schema = StructType([
    StructField("AirportCode", StringType(), True),
    StructField("Position", StructType([
        StructField("Coordinate", StructType([
            StructField("Latitude", StringType(), True),
            StructField("Longitude", StringType(), True),
        ]), True)
    ]), True),
    StructField("CityCode", StringType(), True),
    StructField("CountryCode", StringType(), True),
    StructField("LocationType", StringType(), True),
    StructField("Names", StructType([
        StructField("Name", ArrayType(name_schema), True)
    ]), True),
    StructField("UtcOffset", StringType(), True),
    StructField("TimeZoneId", StringType(), True),
])

airport_resource_schema = StructType([
    StructField("AirportResource", StructType([
        StructField("Airports", StructType([
            StructField("Airport", ArrayType(airport_schema), True)
        ]), True),
        StructField("Meta", meta_schema, True),
    ]), True),
])

airline_names_schema = StructType([
    StructField(
        "Name", name_schema, True)
])

airline_schema = StructType([
    StructField("AirlineID", StringType(), True),
    StructField("AirlineID_ICAO", StringType(), True),
    StructField("Names", airline_names_schema, True),
])


airlines_schema = StructType([
    StructField("Airline",
        ArrayType(airline_schema),
        True
    )
])


airline_resource_schema = StructType([
    StructField(
        "AirlineResource", StructType([
            StructField("Airlines", airlines_schema, True),
            StructField("Meta", meta_schema, True),
        ]),
        True
    )
])


aircraft_summary_schema = StructType([
    StructField("AircraftCode", StringType(), True),
    StructField("Names", StructType([
        StructField("Name", ArrayType(name_schema), True)
    ]), True),
    StructField("AirlineEquipCode", StringType(), True),
])

aircraft_resource_schema = StructType([
    StructField("AircraftSummaries", StructType([
        StructField("AircraftSummary", ArrayType(aircraft_summary_schema), True)
    ]), True),
    StructField("Meta", meta_schema, True),
])

