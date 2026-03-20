from src.client.lufthansa_oauth_client import LufthansaOAuthClient
from src.config.config_properties import get_ConfigProperties
from src.app.secrets import get_secret

from functools import lru_cache


# TODO: cache ?
@lru_cache(maxsize=1)
def get_lufthansa_client():
    configProperties = get_ConfigProperties()
    base_url = f"{configProperties.api.base_url}/{configProperties.api.version}"
    url_oauth_token = configProperties.api.url_oauth_token
    client_id = get_secret(configProperties.secrets.oauth_token_client_id)
    client_secret = get_secret(configProperties.secrets.oauth_token_client_secret)

    # return LufthansaProxyClient(f"{configProperties.api.base_url}/{configProperties.api.version}")
    return LufthansaOAuthClient(base_url, url_oauth_token, client_id, client_secret)
