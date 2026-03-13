import requests

class BaseHttpClient:
    def __init__(self, base_url, timeout=30):
        self.base_url = base_url
        self.defaultTimeout = timeout
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.session = requests.Session()

    def get(self, path, params=None, headers=None, timeout=None):
        if timeout is None:
            timeout = self.defaultTimeout
        response = self.session.get(
            f"{self.base_url}/{path}",
            headers=self.headers, 
            params=params,
            timeout=timeout)

        response.raise_for_status()
        return response.json()

    def post(self, path, params=None, json=None, headers=None, timeout=None):
        if timeout is None:
            timeout = self.defaultTimeout

        response = self.session.post(
            f"{self.base_url}/{path}",
            params=params,
            headers=self.headers,
            json=json,
            timeout=timeout)

        response.raise_for_status()
        return response.json()
