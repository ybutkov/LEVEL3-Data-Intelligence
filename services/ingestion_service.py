from endpoints import get_endpoint
from app.clients import get_lufthansa_client
from app.config import get_config
from utils.ingestion_utils import *
import datetime


def fetch_data(url, query_params=None, max_retries=3, timeout=20):
    if not url:
        raise ValueError(f"Empty url: {url}")

    httpClient = get_lufthansa_client()
    for attempt in range(1, max_retries + 1):
        try:
            response = httpClient.get(
                url,
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

def get_full_save_path(endpoint, offset=None, time_dir=None):
    cfg = get_config()
    name = endpoint.value
    filename = name
    if offset or offset == 0:
        filename = f"{filename}_{offset}"
    filename = f"{filename}.json"
    if time_dir:
        # month = datetime.datetime.now().strftime(ts_format)
        target_dir = f"{cfg.landing.landing_root}/{name}/{time_dir}"
        # print(target_dir)
    else:
        target_dir = f"{cfg.landing.landing_root}/{name}"
    full_save_path = f"{target_dir}/{filename}"
    # print(f"get_full_save_path combine path={full_save_path} offset={offset}")
    return full_save_path

def build_url_for_endpoint(endpoint, path_params=None):
    return get_endpoint(endpoint, path_params)

def get_and_save_all_pages(
        endpoint,
        path_params=None,
        query_params=None,
        limit=100,
        time_dir_format=None):
    
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
        url = build_url_for_endpoint(endpoint, page_query_params)
        response = fetch_data(
            url=url,
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

def get_and_save_data(endpoint, path_params=None, query_params=None, limit=100):
    if endpoint in cfg.LIST_ENDPOINTS:
        return get_and_save_all_pages(
            endpoint=endpoint,
            path_params=path_params,
            query_params=query_params,
            limit=limit,
        )

    return get_and_save_one(
        endpoint=endpoint,
        path_params=path_params,
        query_params=query_params,
    )