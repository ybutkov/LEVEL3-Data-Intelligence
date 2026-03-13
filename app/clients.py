from functools import lru_cache
from clients.lufthansa_proxy_client import LufthansaProxyClient

@lru_cache(maxsize=1)
def get_lufthansa_client():
    return LufthansaProxyClient()
