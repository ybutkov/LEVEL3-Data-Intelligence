from src.client.base_http_client import BaseHttpClient
from src.app.secrets import get_proxy_password


class LufthansaProxyClient(BaseHttpClient):
    def _get_headers(self, headers=None):
        base_headers = {'user-agent': 'my-agent/1.0.1',
            # TODO: Inject password
            'password': get_proxy_password(),
            'Accept': 'application/json'}
        if headers:
            base_headers.update(headers)
        return base_headers
    
    def get(self, path, params=None, headers=None, timeout=None):
        return super().get(path, params, self._get_headers(headers), timeout)
    
    def post(self, path, params=None, json=None, headers=None, timeout=None):
        return super().post(path, params, json, self._get_headers(headers), timeout)
    