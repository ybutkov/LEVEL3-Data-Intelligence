from enum import Enum

class ProfileKeys(Enum):
    LANDING_ROOT = "landing_root",
    BRONZE_ROOT = "bronze_tables",
    TS_FORMAT = "ts_format",
    TS_DAYLY_FORMAT = "ts_dayly_format",
    TS_MONTH_FORMAT = "ts_monthly_format"

PROXY_BASE_URL = "https://lh-proxy.onrender.com"
# API_BASE_PREFIX = "/references"
API_VERSION = "/v1"
SECRET_SCOPE = "lufthansa_proxy"
PROXY_PASSWORD_KEY = "password"
PROXY_PASSWORD_VALUE = "DataIntelligence2026"

# VOLUME_ROOT = "/Volumes/main/lufthansa_level3/staging_area"
# DEFAULT_SCOPE = "/tmp"

PROFILE_TMP = {
    # ProfileKeys.VOLUME_ROOT: "/Volumes/main/lufthansa_level3/staging_area",
    ProfileKeys.LANDING_ROOT: "/Volumes/main/lufthansa_bronze/landing_area/tmp",
    ProfileKeys.BRONZE_ROOT: "main.lufthansa_bronze.{entity_name}",
    ProfileKeys.TS_FORMAT: "%Y%m%dT%H%M%S",
    ProfileKeys.TS_DAYLY_FORMAT: "%Y-%m-%d",
    ProfileKeys.TS_MONTH_FORMAT: "%Y-%m"
}

PROFILE = PROFILE_TMP

# Endpoints keys
class EndpointKeys(Enum):
    AIRPORTS = "airports"
    AIRPORT_CODE = "airport_code"
    COUNTRIES = "countries"
    COUNTRY_CODE = "country_code"
    CITIES = "cities"
    CITY_CODE = "city_code"
    AIRLINES = "airlines"
    AIRLINE_CODE = "airline_code"
    AIRCRAFT = "aircraft"
    AIRCRAFT_CODE = "aircraft_code"
    FLIGHTSTATUS_BY_ROUTE = "flightstatus_by_route"

# Endpoints values
ENDPOINTS = {
    EndpointKeys.AIRPORTS: "/references/airports",
    EndpointKeys.AIRPORT_CODE: "/references/airports/{airportCode}",
    EndpointKeys.COUNTRIES: "/references/countries",
    EndpointKeys.COUNTRY_CODE: "/references/countries/{countryCode}",
    EndpointKeys.CITIES: "/references/cities",
    EndpointKeys.CITY_CODE: "/references/cities/{cityCode}",
    EndpointKeys.AIRLINES: "/references/airlines",
    EndpointKeys.AIRLINE_CODE: "/references/airlines/{airlineCode}",
    EndpointKeys.AIRCRAFT: "/references/aircraft",
    EndpointKeys.AIRCRAFT_CODE: "/references/aircraft/{aircraftCode}",
    EndpointKeys.FLIGHTSTATUS_BY_ROUTE: "/operations/flightstatus/route/{origin}/{destination}/{date}",
}

LIST_ENDPOINTS = {
    EndpointKeys.AIRPORTS,
    EndpointKeys.COUNTRIES,
    EndpointKeys.CITIES,
    EndpointKeys.AIRLINES,
    EndpointKeys.AIRCRAFT,
}