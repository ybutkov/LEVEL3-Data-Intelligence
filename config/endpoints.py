from enum import Enum
from dataclasses import dataclass, field
from typing import Any
from dataclasses import dataclass
from typing import List
from typing import Optional
from config.config_properties import load_yaml
from config.config_properties import ConfigProperties
from util.json_utils import get_value_by_path
import yaml


_ENDPOINTS_CONFIG_PATH = "resources/endpoints.yaml"
_ENDPOINTS_ROOT = "endpoints"
_ENDPOINTS_CONFIGS = None

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
    FLIGHT_SCHEDULES = "flight_schedules"
    
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

    path_params: tuple[str, ...] = ()
    query_params: tuple[str, ...] = ()
    paginable: bool = True
    validation_path: Optional[tuple] = None

    def filter_query_params(self, params: dict | None)-> dict:
        params = params or {}
        if not self.query_params:
            return {}
        return { k: v for k, v in params.items() if k in self.query_params }

    def filter_path_params(self, params: dict | None)-> dict:
        params = params or {}
        if not self.path_params:
            return {}
        return { k: v for k, v in params.items() if k in self.path_params }

    def build_endpoint_path(self, path_params: str) -> str:
        return self.path.format(self.filter_path_params(path_params))

    def build_file_name(self, offset: Optional[int] = None, 
                        limit: Optional[int] = None, timestamp: Optional[str] = None) -> str:
        pass

    def build_landing_path(self, configProperties: ConfigProperties) -> str:
        return configProperties.path_template.landing_dir.format(
            catalog=configProperties.storage.catalog,
            bronze_schema=configProperties.storage.bronze_schema,
            landing_volume=configProperties.storage.landing_volume,
            raw_folder=self.raw_folder
        )

    def build_schema_location(self, storage_cfg, load_type: str) -> str:
        return (
            f"/Volumes/{storage_cfg.catalog}/"
            f"{storage_cfg.bronze_schema}/"
            f"{storage_cfg.metadata_volume}/"
            f"{self.raw_folder}/schema/{load_type}"
        )

    def build_checkpoint_location(self, storage_cfg, load_type: str) -> str:
        return (
            f"/Volumes/{storage_cfg.catalog}/"
            f"{storage_cfg.bronze_schema}/"
            f"{storage_cfg.metadata_volume}/"
            f"{self.raw_folder}/checkpoint/{load_type}"
        )

    def build_bronze_table_name(self, storage_cfg) -> str:
        return f"{storage_cfg.catalog}.{storage_cfg.bronze_schema}.{self.bronze_table}"

    def build_silver_table_name(self, storage_cfg) -> str:
        return f"{storage_cfg.catalog}.{storage_cfg.silver_schema}.{self.silver_table}"
    
    def is_valid_response(self, data) -> bool:
        if data is None:
            return False
        if self.resource_key is not None:
            return get_value_by_path(data, (self.resource_key,)) is not None
        if self.validation_path is not None:
            return get_value_by_path(data, self.validation_path) is not None
        # TODO: add more checks? or just return True. Last checking?
        return isinstance(data, (dict, list))
    

def load_endpoint_configs():
    cfg = load_yaml(_ENDPOINTS_CONFIG_PATH)
    endpoints = cfg.get(_ENDPOINTS_ROOT, {})

    result = {}
    for key_str, data in endpoints.items():
        key = EndpointKeys(key_str)
        result[key] = EndpointConfig(
            key=key,
            path=data["path"],

            resource_key=data.get("resource_key"),
            collection_path=tuple(data.get("collection_path", [])),
            total_count_path=tuple(data.get("total_count_path", [])),

            raw_folder=data.get("raw_folder", ""),
            bronze_table=data.get("bronze_table", ""),
            silver_table=data.get("silver_table", ""),

            path_params=tuple(data.get("path_params", [])),
            query_params=tuple(data.get("query_params", [])),

            paginable=data.get("paginable", True),
            validation_path=tuple(data["validation_path"]) if data.get("validation_path") else None,
        )
        print(key.value)
    print(result[EndpointKeys.COUNTRIES].path)
    return result

# ENDPOINT_CONFIGS: dict[EndpointKeys, EndpointConfig] = {
#     EndpointKeys.COUNTRIES: EndpointConfig(
#         key=EndpointKeys.COUNTRIES,
#         path="/references/countries",
#         resource_key="CountryResource",
#         collection_path=("CountryResource", "Countries", "Country"),
#         total_count_path=("CountryResource", "Meta", "TotalCount"),
#         raw_folder="countries",
#         bronze_table="countries_raw",
#         silver_table="countries",
#         query_params=("offset", "limit", "lang"),
#         paginable=True,
#     ),
#         EndpointKeys.CITIES: EndpointConfig(
#         key=EndpointKeys.CITIES,
#         path="/references/cities",
#         resource_key="CityResource",
#         collection_path=("CityResource", "Cities", "City"),
#         total_count_path=("CityResource", "Meta", "TotalCount"),
#         raw_folder="cities",
#         bronze_table="cities_raw",
#         silver_table="cities",
#         query_params=("offset", "limit", "lang"),
#         paginable=True,
#     ),
#     EndpointKeys.AIRPORTS: EndpointConfig(
#         key=EndpointKeys.AIRPORTS,
#         path="/references/airports",
#         resource_key="AirportResource",
#         collection_path=("Airports", "Airport"),
#         total_count_path=("AirportResource", "Meta", "TotalCount"),
#         raw_folder="airports",
#         bronze_table="airports_raw",
#         silver_table="airports",
#         query_params=("offset", "limit", "lang"),
#         paginable=True,
#     ),
#     EndpointKeys.AIRLINES: EndpointConfig(
#         key=EndpointKeys.AIRLINES,
#         path="/references/airlines",
#         resource_key="AirlineResource",
#         collection_path=("Airlines", "Airline"),
#         total_count_path=("AirlineResource", "Meta", "TotalCount"),
#         raw_folder="airlines",
#         bronze_table="airlines_raw",
#         silver_table="airlines",
#         query_params=("offset", "limit", "lang"),
#         paginable=True,
#     ),
#     EndpointKeys.AIRCRAFT: EndpointConfig(
#         key=EndpointKeys.AIRCRAFT,
#         path="/references/aircraft",
#         resource_key="AircraftResource",
#         collection_path=("AircraftSummaries", "AircraftSummary"),
#         total_count_path=("AircraftResource", "Meta", "TotalCount"),
#         raw_folder="aircraft",
#         bronze_table="aircraft_raw",
#         silver_table="aircraft",
#         query_params=("offset", "limit", "lang"),
#         paginable=True,
#     ),
#     EndpointKeys.FLIGHTSTATUS_BY_ROUTE: EndpointConfig(
#         key=EndpointKeys.FLIGHTSTATUS_BY_ROUTE,
#         path="/operations/flightstatus/route/{departure_airport_code}/{arrival_airport_code}/{date}",
#         resource_key="FlightStatusResource",
#         collection_path=("FlightStatusResource", "Flights", "Flight"),
#         total_count_path=("FlightStatusResource", "Meta", "TotalCount"),
#         raw_folder="flightstatus_by_route",
#         bronze_table="flightstatus_by_route_raw",
#         silver_table="flightstatus_by_route",
#         path_params=("departure_airport_code", "arrival_airport_code", "date"),
#         query_params=("offset", "limit", "lang"),
#         paginable=True,
#     ),
#     # https://lh-proxy.onrender.com/v1/flight-schedules/flightschedules/passenger?airlines=LH&startDate=10MAR26&endDate=15MAR26&daysOfOperation=1234567&timeMode=UTC
#     EndpointKeys.FLIGHT_SCHEDULES: EndpointConfig(
#         key=EndpointKeys.FLIGHT_SCHEDULES,
#         path="/flight-schedules/flightschedules/passenger",
#         resource_key=None,
#         collection_path=None,
#         total_count_path=None,
#         raw_folder="flight_schedules",
#         bronze_table="flight_schedules_raw",
#         silver_table="flight_schedules",
#         query_params=("airlines", "flightNumberRanges", "startDate", "endDate", "daysOfOperation", "timeMode"),
#         paginable=False,
#         validation_path=(0, "airline"),
#     ),
# }

def get_endpoint_config(endpoint_key: EndpointKeys) -> EndpointConfig:
    global _ENDPOINTS_CONFIGS
    if _ENDPOINTS_CONFIGS is None:
        _ENDPOINTS_CONFIGS = load_endpoint_configs()
    return _ENDPOINTS_CONFIGS[endpoint_key]

   


# def get_endpoint_config(endpoint_key: EndpointKeys) -> EndpointConfig:
#     return ENDPOINT_CONFIGS[endpoint_key]


# def build_endpoint_path(endpoint_key: EndpointKeys, **path_params: str) -> str:
#     config = get_endpoint_config(endpoint_key)
#     return config.path.format(**path_params)
