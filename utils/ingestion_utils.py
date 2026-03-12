import requests
import config.config as cfg
from utils.secrets import *
import datetime
import time
from utils.landing import *

# def get_data(path, ver="/v1"):
#     headers = {'user-agent': 'my-agent/1.0.1',
#                'password': get_proxy_password(),
#                'Accept': 'application/json'}
#     full_url = f"{PROXY_BASE_URL}{ver}{API_BASE_PREFIX}{path}"
#     response = requests.get(full_url, headers=headers)
#     return response

def get_total_count(data):
    resource = next(iter(data.values()), {})
    return resource.get("Meta", {}).get("TotalCount")

def get_headers():
    return {'user-agent': 'my-agent/1.0.1',
            'password': get_proxy_password(),
            'Accept': 'application/json'}

def get_and_save_data(endpoint):
    response = get_data(endpoint)
    data = response.json()
    full_save_path = get_full_save_path(endpoint, timestamp=True)
    save_json_with_dbutils(data, full_save_path, True, 2)

def get_and_save_one(endpoint, path_params=None, query_params=None):
    response = fetch_data(
        endpoint,
        path_params=path_params,
        query_params=query_params,
    )
    data = response.json()
    full_save_path = get_full_save_path(
        endpoint=endpoint,
        timestamp=True,
    )
    save_json_with_dbutils(data, full_save_path, True, 2)

def get_data(endpoint, path_params=None, query_params=None):
    headers = get_headers()
    path_template = cfg.ENDPOINTS.get(endpoint)
    if path_params:
        full_path = path_template.format(**path_params)
    else:
        full_path = path_template
    full_url = f"{cfg.PROXY_BASE_URL}{cfg.API_VERSION}{full_path}"
    # fill query params
    response = requests.get(full_url, headers=headers, params=query_params)
    return response

def get_flights(origin, destination, date, query_params=None):
    headers = get_headers()
    path_template = cfg.ENDPOINTS.get(cfg.EndpointKeys.FLIGHTSTATUS_BY_ROUTE)
    full_path = path_template.format(origin=origin, destination=destination, date=date)
    full_url = f"{cfg.PROXY_BASE_URL}{cfg.API_VERSION}{full_path}"
    response = requests.get(full_url, headers=headers, params=query_params)
    return response

def fetch_data(endpoint, path_params=None, query_params=None, max_retries=3, timeout=20):
    headers = get_headers()
    path_template = cfg.ENDPOINTS.get(endpoint)

    if not path_template:
        raise ValueError(f"Unknown endpoint: {endpoint}")

    if path_params:
        full_path = path_template.format(**path_params)
    else:
        full_path = path_template
    full_url = f"{cfg.PROXY_BASE_URL}{cfg.API_VERSION}{full_path}"

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                full_url,
                headers=headers,
                params=query_params,
                timeout=timeout,
            )

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == max_retries:
                    response.raise_for_status()
                time.sleep(2 ** (attempt - 1))
                continue

            response.raise_for_status()
            return response

        except (requests.Timeout, requests.ConnectionError):
            if attempt == max_retries:
                raise
            time.sleep(2 ** (attempt - 1))


def get_and_save_all_pages(
    endpoint,
    path_params=None,
    query_params=None,
    limit=100,
    time_dir_format=None
):
    if query_params:
        query_params = query_params.copy() 
    else:
        query_params = {}

    offset = 0
    total = None

    while True:
        page_query_params = {
            **query_params,
            "limit": limit,
            "offset": offset,
        }

        response = fetch_data(
            endpoint,
            path_params=path_params,
            query_params=page_query_params,
        )

        data = response.json()
        print(f"Loading page {offset} from {total}")
        if total is None:
            total = get_total_count(data)
            if total is None:
                raise ValueError("Response does not contain 'total'")

        # items = data.get("data")
        # if items is None:
        #     raise ValueError("Response does not contain 'data'")
        time_dir = datetime.datetime.now().strftime(time_dir_format)
        # print(time_dir)
        full_save_path = get_full_save_path(
            endpoint=endpoint,
            offset=offset,
            time_dir=time_dir
        )
        # print(full_save_path)

        save_json_with_dbutils(data, full_save_path, True, 2)

        offset += limit

        if offset >= total:
            break

        # if not items:
        #     raise ValueError(
        #         f"Empty page received before reaching total. offset={offset}, total={total}"
        #     )

