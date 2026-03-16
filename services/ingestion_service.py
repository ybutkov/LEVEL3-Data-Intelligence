from app.clients import get_lufthansa_client
from config.config_properties import get_ConfigProperties
# from utils.ingestion_utils import *
import datetime
import time
import requests
from config.endpoints import get_endpoint_config
from config.endpoints import build_endpoint_path
from util.json_utils import get_value_by_path
from services.storage_service import save_json_with_dbutils
from app.logger import get_logger
from http import HTTPStatus


def fetch_data(url, query_params=None, max_retries=3, timeout=20):
    logger = get_logger()
    if not url:
        logger.error(f"Empty url: {url}")
        raise ValueError(f"Empty url: {url}")

    httpClient = get_lufthansa_client()
    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"Request to {url} attempt #{attempt}")
            response = httpClient.get(
                url,
                params=query_params,
                timeout=timeout,
            )

            if response.status_code in {HTTPStatus.TOO_MANY_REQUESTS,
                                        HTTPStatus.INTERNAL_SERVER_ERROR,
                                        HTTPStatus.BAD_GATEWAY,
                                        HTTPStatus.SERVICE_UNAVAILABLE,
                                        HTTPStatus.GATEWAY_TIMEOUT}:
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

def get_full_save_path(endpoint, page=None, offset=None, limit=None, time_period=None):
    config = get_endpoint_config(endpoint)

    configProperties = get_ConfigProperties()

    landing_dir = configProperties.path_template.landing_dir.format(
        catalog=configProperties.storage.catalog,
        bronze_schema=configProperties.storage.bronze_schema,
        landing_volume=configProperties.storage.landing_volume,
        entity=endpoint.value,
    )
    if time_period in ("daily", "monthly"):
        # time_dir = datetime.datetime.now().strftime(cfg.path_template.time_template.monthly)
        time_dir = datetime.datetime.now().strftime(configProperties.path_template.time_template[time_period])
    else:
        time_dir = ""
    if offset is None or limit is None:
        file_name = configProperties.path_template.file_name_single
    else:
        file_name = configProperties.path_template.file_name.format(
            page=page,
            offset=offset,
            limit=limit
            )
    full_save_path = f"{landing_dir}{time_dir}{file_name}"
    return full_save_path

# def get_full_save_path_old(endpoint, offset=None, time_dir=None):
#     cfg = get_config()
#     name = endpoint.value
#     filename = name
#     if offset or offset == 0:
#         filename = f"{filename}_{offset}"
#     filename = f"{filename}.json"
#     if time_dir:
#         target_dir = f"{cfg.landing.landing_root}/{name}/{time_dir}"
#     else:
#         target_dir = f"{cfg.landing.landing_root}/{name}"
#     full_save_path = f"{target_dir}/{filename}"
#     # print(f"get_full_save_path combine path={full_save_path} offset={offset}")
#     return full_save_path

def build_url_for_endpoint(endpoint, path_params=None):
    return build_endpoint_path(endpoint, **(path_params or {}))

def get_total_count(data):
    resource = next(iter(data.values()), {})
    return resource.get("Meta", {}).get("TotalCount")

# endpoint -> url
def get_split_and_save_request(
        endpoint,
        path_params=None,
        query_params=None,
        offset=0,
        limit=20,
        time_period=None,
        failed_offsets=None):
    
    logger = get_logger(__name__)
    endpoint_config = get_endpoint_config(endpoint)
    if failed_offsets is None:
        failed_offsets = []
    data = None
    path_params = path_params or {}
    query_params = query_params or {}

    try:
        logger.info(f"Request: {endpoint.value} offset={offset} limit={limit}")
        page_query_params = {
            **query_params,
            "limit": limit,
            "offset": offset,
        }
        url = build_url_for_endpoint(endpoint, path_params or {})
        response = fetch_data(
            url=url,
            query_params=page_query_params,
        )
        data = response.json()
        if (get_value_by_path(data, (endpoint_config.resource_key,)) is None):
            raise ValueError("Fetching Error")
        full_save_path = get_full_save_path(
            endpoint=endpoint,
            offset=offset,
            limit=limit,
            time_period=time_period
        )
        save_json_with_dbutils(data, full_save_path, True, 2)
        logger.info(f"Saved endpoint={endpoint.value} offset={offset} limit={limit} path={full_save_path}")
        return data, failed_offsets

    except Exception as e:
        logger.exception(f"Request failed endpoint={endpoint.value} offset={offset} limit={limit} error={e}")
        if limit <= 1:
            logger.error(f"Skipping bad record endpoint={endpoint.value} offset={offset} limit={limit}")
            # TODO: Should write to file error json ?
            failed_offsets.append(offset)
            return data, failed_offsets
        left_part = limit // 2
        right_part = limit - left_part
        # TODO: Check left_part and right_part ?

        _, failed_offsets = get_split_and_save_request(
            endpoint=endpoint,
            path_params=path_params,
            query_params=query_params,
            offset=offset,
            limit=left_part,
            time_period=time_period,
            failed_offsets=failed_offsets,
        )
        _, failed_offsets = get_split_and_save_request(
            endpoint=endpoint,
            path_params=path_params,
            query_params=query_params,
            offset=offset + left_part,
            limit=right_part,
            time_period=time_period,
            failed_offsets=failed_offsets,
        )
        return data, failed_offsets

def get_and_save_all_pages(
        endpoint,
        path_params=None,
        query_params=None,
        limit=100,
        time_period=None):
    
    logger = get_logger(__name__)
    logger.info(f"Start retrieving data for {endpoint.value}")
    if query_params:
        query_params = query_params.copy() 
    else:
        query_params = {}
    page = 1
    offset = 0
    total = None
    endpoint_config = get_endpoint_config(endpoint)
    failed_offsets = []

    while True:
        data, failed_offsets = get_split_and_save_request(endpoint, 
                                                    path_params=path_params,
                                                    query_params=query_params,
                                                    offset=offset,
                                                    limit=limit,
                                                    time_period=time_period,
                                                    failed_offsets=failed_offsets)
      
        # TODO: total started from None !!!
        # logger.info(f"From {url} loaded page {offset} from {total}")
        
        if total is None:
            total = get_value_by_path(data, endpoint_config.total_count_path)
        if total is None:
            # TODO: rewrite to load until empty
            logger.error(f"Response does not contain 'total'")
            # raise ValueError("Response does not contain 'total'")

        offset += limit
        page += 1

        # TODO: Delete. rewrite to load until empty
        # if page > 4:
        #     print("Failed offsets:", failed_offsets)
        #     break

        # TODO: Add loading until offset >= total or empty result
        if total and offset >= total:
            if failed_offsets:
                logger.warning(f"Failed offsets: {failed_offsets}. Sent to Kafka")
            break
    logger.info(f"Finish retrieving data for {endpoint.value}")

# def get_flights(origin, destination, date, query_params=None):
#     headers = get_headers()
#     path_template = cfg.ENDPOINTS.get(cfg.EndpointKeys.FLIGHTSTATUS_BY_ROUTE)
#     full_path = path_template.format(origin=origin, destination=destination, date=date)
#     full_url = f"{cfg.PROXY_BASE_URL}{cfg.API_VERSION}{full_path}"
#     response = requests.get(full_url, headers=headers, params=query_params)
#     return response

# def get_and_save_data(endpoint, path_params=None, query_params=None, limit=100):
#     if endpoint in cfg.LIST_ENDPOINTS:
#         return get_and_save_all_pages(
#             endpoint=endpoint,
#             path_params=path_params,
#             query_params=query_params,
#             limit=limit,
#         )

#     return get_and_save_one(
#         endpoint=endpoint,
#         path_params=path_params,
#         query_params=query_params,
#     )