from functools import lru_cache
from client.lufthansa_proxy_client import LufthansaProxyClient
from config.config_properties import get_ConfigProperties

@lru_cache(maxsize=1)
def get_lufthansa_client():
    configProperties = get_ConfigProperties()
    return LufthansaProxyClient(f"{configProperties.api.base_url}/{configProperties.api.version}")
