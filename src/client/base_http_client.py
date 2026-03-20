import requests


class BaseHttpClient:
    def __init__(self, base_url, timeout=30):
        self.base_url = base_url
        self.timeout = timeout
        self.headers = {
            'user-agent': 'my-agent/1.0.1',
            'Accept': 'application/json',
            'Content-Type': 'application/json'            
        }
        self.session = requests.Session()

    def get(self, path, params=None, headers=None, timeout=None):
        if timeout is None:
            timeout = self.timeout
        if headers is None:
            headers = self.headers
        response = self.session.get(
            f"{self.base_url}{path}",
            headers=headers, 
            params=params,
            timeout=timeout)
        response.raise_for_status()
        return response

    def post(self, path, params=None, json=None, headers=None, timeout=None):
        if timeout is None:
            timeout = self.timeout
        headers = headers or self.headers
        response = self.session.post(
            f"{self.base_url}{path}",
            params=params,
            headers=self.headers,
            json=json,
            timeout=timeout)

        response.raise_for_status()
        return response
