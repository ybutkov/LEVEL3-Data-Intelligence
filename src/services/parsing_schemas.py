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

single_name_schema = StructType([
    StructField(
        "Name", name_schema, True)
])

airline_schema = StructType([
    StructField("AirlineID", StringType(), True),
    StructField("AirlineID_ICAO", StringType(), True),
    StructField("Names", single_name_schema, True),
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


aircraft_schema = StructType([
    StructField("AircraftCode", StringType(), True),
    StructField("Names", single_name_schema, True),
    StructField("AirlineEquipCode", StringType(), True),
])

aircraft_summary_schema = StructType([
    StructField("AircraftSummary",
        ArrayType(aircraft_schema),
        True
    )
])

aircraft_resource_schema = StructType([
    StructField(
        "AircraftResource", StructType([
            StructField("AircraftSummaries", aircraft_summary_schema, True),
            StructField("Meta", meta_schema, True),
        ]),
        True
    )
])

# Operations Schemas

date_time_schema = StructType([
    StructField("DateTime", StringType(), True),
])

time_status_schema = StructType([
    StructField("Code", StringType(), True),
    StructField("Definition", StringType(), True),
])

terminal_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Gate", StringType(), True),
])

departure_arrival_schema = StructType([
    StructField("AirportCode", StringType(), True),

    StructField("ScheduledTimeLocal", date_time_schema, True),
    StructField("ScheduledTimeUTC", date_time_schema, True),

    StructField("ActualTimeLocal", date_time_schema, True),
    StructField("ActualTimeUTC", date_time_schema, True),

    StructField("EstimatedTimeLocal", date_time_schema, True),
    StructField("EstimatedTimeUTC", date_time_schema, True),

    StructField("TimeStatus", time_status_schema, True),
    StructField("Terminal", terminal_schema, True),
])

carrier_schema = StructType([
    StructField("AirlineID", StringType(), True),
    StructField("FlightNumber", StringType(), True),
])

equipment_schema = StructType([
    StructField("AircraftCode", StringType(), True),
    StructField("AircraftRegistration", StringType(), True),
])

flight_status_schema = StructType([
    StructField("Code", StringType(), True),
    StructField("Definition", StringType(), True),
])

flight_schema = StructType([
    StructField("Departure", departure_arrival_schema, True),
    StructField("Arrival", departure_arrival_schema, True),

    StructField("MarketingCarrier", carrier_schema, True),
    StructField("OperatingCarrier", carrier_schema, True),

    StructField("Equipment", equipment_schema, True),
    StructField("FlightStatus", flight_status_schema, True),

    StructField("ServiceType", StringType(), True),
])

meta_link_schema = StructType([
    StructField("@Href", StringType(), True),
    StructField("@Rel", StringType(), True),
])

meta_schema = StructType([
    StructField("@Version", StringType(), True),
    StructField("Link", ArrayType(meta_link_schema), True),
    StructField("TotalCount", StringType(), True),
])

flight_status_resource_schema = StructType([
    StructField("FlightStatusResource", StructType([
        StructField("Flights", StructType([
            StructField("Flight", ArrayType(flight_schema), True),
        ]), True),
        StructField("Meta", meta_schema, True),
    ]), True),
])
