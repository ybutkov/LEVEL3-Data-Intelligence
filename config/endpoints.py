from enum import Enum
from dataclasses import dataclass, field
from typing import Any
from dataclasses import dataclass
from typing import List

class EndpointKeys(str, Enum):
    AIRPORTS = "airports"
    AIRPORT_BY_CODE = "airport_code"
    COUNTRIES = "countries"
    COUNTRY_BY_CODE = "country_code"
    CITIES = "cities"
    CITY_BY_CODE = "city_code"
    AIRLINES = "airlines"
    AIRLINE_BY_CODE = "airline_code"
    AIRCRAFT = "aircraft"
    AIRCRAFT_BY_CODE = "aircraft_code"
    FLIGHTSTATUS_BY_ROUTE = "flightstatus_by_route"
    
@dataclass(frozen=True)
class EndpointConfig:
    key: EndpointKeys
    path: str

    resource_key: str | None = None
    collection_path: tuple[str, ...] = ()
    total_count_path: tuple[str, ...] = ()

    raw_folder: str = ""
    bronze_table: str = ""
    silver_table: str = ""

    is_list: bool = False
    supports_pagination: bool = False

    path_params: tuple[str, ...] = ()
    query_params: tuple[str, ...] = ()

    default_query_params: dict[str, Any] = field(default_factory=dict)

ENDPOINT_CONFIGS: dict[EndpointKeys, EndpointConfig] = {
    EndpointKeys.COUNTRIES: EndpointConfig(
        key=EndpointKeys.COUNTRIES,
        path="/references/countries",
        resource_key="CountryResource",
        collection_path=("CountryResource", "Countries", "Country"),
        total_count_path=("CountryResource", "Meta", "TotalCount"),
        raw_folder="countries",
        bronze_table="countries_raw",
        silver_table="countries",
        is_list=True,
        supports_pagination=True,
        query_params=("offset", "limit", "lang"),
        default_query_params={"lang": "en"},
    ),
        EndpointKeys.CITIES: EndpointConfig(
        key=EndpointKeys.CITIES,
        path="/references/cities",
        resource_key="CityResource",
        collection_path=("CityResource", "Cities", "City"),
        total_count_path=("CityResource", "Meta", "TotalCount"),
        raw_folder="cities",
        bronze_table="cities_raw",
        silver_table="cities",
        is_list=True,
        supports_pagination=True,
        query_params=("offset", "limit", "lang"),
        default_query_params={"lang": "en"},
    ),
    EndpointKeys.AIRPORTS: EndpointConfig(
        key=EndpointKeys.AIRPORTS,
        path="/references/airports",
        resource_key="AirportResource",
        collection_path=("Airports", "Airport"),
        total_count_path=("AirportResource", "Meta", "TotalCount"),
        raw_folder="airports",
        bronze_table="airports_raw",
        silver_table="airports",
        is_list=True,
        supports_pagination=True,
        query_params=("offset", "limit", "lang"),
        default_query_params={"lang": "en"},
    ),
    EndpointKeys.AIRLINES: EndpointConfig(
        key=EndpointKeys.AIRLINES,
        path="/references/airlines",
        resource_key="AirlineResource",
        collection_path=("Airlines", "Airline"),
        total_count_path=("AirlineResource", "Meta", "TotalCount"),
        raw_folder="airlines",
        bronze_table="airlines_raw",
        silver_table="airlines",
        is_list=True,
        supports_pagination=True,
        query_params=("offset", "limit", "lang"),
        default_query_params={"lang": "en"},
    ),
    EndpointKeys.AIRCRAFT: EndpointConfig(
        key=EndpointKeys.AIRCRAFT,
        path="/references/aircraft",
        resource_key="AircraftResource",
        collection_path=("AircraftSummaries", "AircraftSummary"),
        total_count_path=("AircraftResource", "Meta", "TotalCount"),
        raw_folder="aircraft",
        bronze_table="aircraft_raw",
        silver_table="aircraft",
        is_list=True,
        supports_pagination=True,
        query_params=("offset", "limit", "lang"),
        default_query_params={"lang": "en"},
    ),
}

def get_endpoint_config(endpoint_key: EndpointKeys) -> EndpointConfig:
    return ENDPOINT_CONFIGS[endpoint_key]


def build_endpoint_path(endpoint_key: EndpointKeys, **path_params: str) -> str:
    config = get_endpoint_config(endpoint_key)
    return config.path.format(**path_params)
