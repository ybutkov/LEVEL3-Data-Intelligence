from src.config.config_properties import load_yaml
from src.config.config_properties import ConfigProperties
from src.util.json_utils import get_value_by_path
from enum import Enum
from dataclasses import dataclass, field
from typing import Any
from dataclasses import dataclass
from typing import List
from typing import Optional
import yaml
import datetime


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
    FLIGHTSCHEDULES_BY_ROUTE = "flight_schedules_by_route"
    FLIGHTSTATUS_BY_DEPARTURE = "flightstatus_by_departure"
    FLIGHTSTATUS_BY_ARRIVAL = "flightstatus_by_arrival"
    
@dataclass(frozen=True)
class EndpointConfig:
    key: EndpointKeys
    path: str

    resource_key: str | None = None
    collection_path: tuple[str, ...] = ()
    total_count_path: tuple[str, ...] = ()

    raw_folder: str = ""
    raw_folder_details: str = "2"
    bronze_table: str = ""
    silver_table: str = ""

    path_params: tuple[str, ...] = ()
    query_params: tuple[str, ...] = ()
    paginable: bool = True
    validation_path: Optional[tuple] = None
    stop_on_404: bool = False

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

    def build_endpoint_path(self, path_params: dict) -> str:
        return self.path.format(**self.filter_path_params(path_params))

    def build_full_file_name(self, configProperties: ConfigProperties, path_params: dict,
                        page: Optional[int] = None, offset: Optional[int] = None, 
                        limit: Optional[int] = None, 
                        time_period: Optional[str] = None) -> str:
        
        landing_dir = self.build_landing_path(configProperties)
        folder_details = self.raw_folder_details.format(**self.filter_path_params(path_params))
        landing_dir = f"{landing_dir}{folder_details}"
        time = datetime.datetime.now()
        # TODO: Make time_period="default" for default value
        if time_period in ("daily", "monthly"):
            # time_dir = datetime.datetime.now().strftime(cfg.path_template.dir_time_template.monthly)
            dir_time = time.strftime(configProperties.path_template.dir_time_template[time_period])
        else:
            dir_time = time.strftime(configProperties.path_template.dir_time_template["default"])
        
        file_name_datetime_prefix = time.strftime(configProperties.path_template.file_name.datetime_prefix)
        if offset is None or limit is None:
            file_name = configProperties.path_template.file_name.single_name
        else:
            file_name = configProperties.path_template.file_name.with_offset.format(
                # TODO: without page ?
                page=page,
                offset=offset,
                limit=limit
                )
        # TODO: Check path
        full_save_path = f"{landing_dir}{dir_time}{file_name_datetime_prefix}{file_name}"
        return full_save_path

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
            f"{storage_cfg.autoloader_volume}/"
            f"schema/{self.raw_folder}"
        )

    def build_checkpoint_location(self, storage_cfg, load_type: str) -> str:
        return (
            f"/Volumes/{storage_cfg.catalog}/"
            f"{storage_cfg.bronze_schema}/"
            f"{storage_cfg.autoloader_volume}/"
            f"checkpoint/{self.raw_folder}"
        )

    def build_bronze_table_name(self, storage_cfg) -> str:
        return f"{storage_cfg.catalog}.{storage_cfg.bronze_schema}.{self.bronze_table}"

    def build_silver_table_name(self, storage_cfg) -> str:
        return f"{storage_cfg.catalog}.{storage_cfg.silver_schema}.{self.silver_table}"
    
    # def is_valid_response(self, data) -> bool:
    #     if data is None:
    #         return False
    #     if self.resource_key is not None:
    #         return get_value_by_path(data, (self.resource_key,)) is not None
    #     if self.validation_path is not None:
    #         return get_value_by_path(data, self.validation_path) is not None
    #     # TODO: add more checks? (collection_path) or just return True. Last checking?
    #     return isinstance(data, (dict, list))
    
    def is_valid_response(self, data) -> bool:
        if data is None:
            return False
        if self.resource_key:
            if get_value_by_path(data, (self.resource_key,)) is None:
                return False
        if self.validation_path:
            if get_value_by_path(data, self.validation_path) is None:
                return False
         # TODO: add more checks? (collection_path) or just return True. Last checking?
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
            raw_folder_details=data.get("raw_folder_details", ""),
            bronze_table=data.get("bronze_table", ""),
            silver_table=data.get("silver_table", ""),

            path_params=tuple(data.get("path_params", [])),
            query_params=tuple(data.get("query_params", [])),

            paginable=data.get("paginable", True),
            validation_path=tuple(data["validation_path"]) if data.get("validation_path") else None,
            stop_on_404=data.get("stop_on_404", False),
        )
    return result


def get_endpoint_config(endpoint_key: EndpointKeys) -> EndpointConfig:
    global _ENDPOINTS_CONFIGS
    if _ENDPOINTS_CONFIGS is None:
        _ENDPOINTS_CONFIGS = load_endpoint_configs()
    return _ENDPOINTS_CONFIGS[endpoint_key]

   