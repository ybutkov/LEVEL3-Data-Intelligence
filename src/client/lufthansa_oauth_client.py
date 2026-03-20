from src.client.base_http_client import BaseHttpClient
from src.app.secrets import get_proxy_password

from datetime import datetime, timedelta
import requests


class LufthansaOAuthClient(BaseHttpClient):
    def __init__(self, base_url, url_oauth_token, client_id, client_secret, timeout=30):
        super().__init__(base_url, timeout)
        self.url_oauth_token = url_oauth_token
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token = None
        self._expires_at = None
    
    def _fetch_token(self):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        response = self.session.post(
            f"{self.url_oauth_token}",
            data=data,
            headers=headers,
            timeout=self.timeout,
        )
        response.raise_for_status() 
        token_data = response.json()
        self._access_token = token_data["access_token"]
        expires_in = int(token_data.get("expires_in", 1800))
        self._expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 60)
    
    def _ensure_token(self):
        if self._access_token and self._expires_at:
            if datetime.utcnow() < self._expires_at:
                return
        self._fetch_token()

    def _get_headers(self, headers=None):
        base_headers = {}
        if headers:
            base_headers.update(headers)
        return base_headers
     
    def get(self, path: str, params=None, headers=None, timeout=None):
        self._ensure_token()
        headers = headers or {}
        headers["Authorization"] = f"Bearer {self._access_token}"
        headers["Accept"] = "application/json"
        try:
            return super().get(path, params=params, headers=headers, timeout=timeout)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response else None
            if status == 401:
                self._fetch_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                return super().get(path, params=params, headers=headers, timeout=timeout)
        raise
    
    # TODO: implement post with token
    def post(self, path, params=None, json=None, headers=None, timeout=None):
        return super().post(path, params, json, self._get_headers(headers), timeout)
    