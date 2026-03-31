import requests


class BaseHttpClient:
    """
    Base HTTP client for making REST API requests.
    
    Manages HTTP GET and POST requests with configurable headers, timeouts,
    and automatic error handling via raise_for_status().
    """
    
    def __init__(self, base_url, timeout=30):
        """
        Initialize the HTTP client with a base URL and timeout.
        
        Args:
            base_url (str): The base URL for all requests
            timeout (int): Request timeout in seconds (default: 30)
        """
        self.base_url = base_url
        self.timeout = timeout
        self.headers = {
            'user-agent': 'my-agent/1.0.1',
            'Accept': 'application/json',
            'Content-Type': 'application/json'            
        }
        self.session = requests.Session()

    def get(self, path, params=None, headers=None, timeout=None):
        """
        Execute a GET request.
        
        Args:
            path (str): URL path to append to base_url
            params (dict): Query parameters (default: None)
            headers (dict): Custom headers (default: None, uses default headers)
            timeout (int): Request timeout in seconds (default: None, uses self.timeout)
            
        Returns:
            requests.Response: Response object
            
        Raises:
            requests.HTTPError: If response status code indicates error
        """
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
        """
        Execute a POST request.
        
        Args:
            path (str): URL path to append to base_url
            params (dict): Query parameters (default: None)
            json (dict): JSON body data (default: None)
            headers (dict): Custom headers (default: None, uses default headers)
            timeout (int): Request timeout in seconds (default: None, uses self.timeout)
            
        Returns:
            requests.Response: Response object
            
        Raises:
            requests.HTTPError: If response status code indicates error
        """
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
