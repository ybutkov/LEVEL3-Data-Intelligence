from clients.base_http_client import BaseHttpClient
from app.secrets import get_proxy_password

class LufthansaProxyClient(BaseHttpClient):
    def _get_headers(self, headers=None):
        base_headres = {'user-agent': 'my-agent/1.0.1',
            'password': get_proxy_password(),
            'Accept': 'application/json'}
        if headers:
            base_headres.update(headers)
        return base_headres
    
    def get(self, path, params=None, headers=None, timeout=None):
        return super()._get(path, params, self._get_headers(headers), timeout)
    
    def post(self, path, params=None, json=None, headers=None, timeout=None):
        return super()._post(path, params, json, self._get_headers(headers), timeout)
    