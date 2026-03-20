from src.app.clients import get_lufthansa_client
from src.config.config_properties import get_ConfigProperties
from src.config.endpoints import get_endpoint_config
from src.util.json_utils import get_value_by_path
from src.services.storage_service import save_json_with_dbutils
from src.app.logger import get_logger

from http import HTTPStatus
import datetime
import time
import requests


logger = get_logger(__name__)

def fetch_data(url, query_params=None, max_retries=3, timeout=20):
    # logger = get_logger()
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
            # if in (429, 500, 502, 503, 504)
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

# endpoint -> url ?
def get_split_and_save_request(
        endpoint,
        path_params=None,
        query_params=None,
        offset=None,
        limit=None,
        time_period=None,
        failed_offsets=None):
    
    # logger = get_logger(__name__)
    endpoint_config = get_endpoint_config(endpoint)
    configProperties = get_ConfigProperties()
    if failed_offsets is None:
        failed_offsets = []
    data = None
    path_params = path_params or {}
    page_query_params = query_params or {}

    try:
        if endpoint_config.paginable:
            logger.info(f"Request: {endpoint.value} offset={offset} limit={limit}")
            page_query_params.update({"limit": limit, "offset": offset})
        else:
            logger.info(f"Request: {endpoint.value}")
        # url = build_url_for_endpoint(endpoint, path_params)
        url = endpoint_config.build_endpoint_path(path_params)
        response = fetch_data(
            url=url,
            query_params=page_query_params,
        )
        data = response.json()
        if not endpoint_config.is_valid_response(data):
            raise ValueError("Fetching Error: response structure is not valid!!")
        full_save_path = endpoint_config.build_full_file_name(
            configProperties=configProperties,
            path_params=path_params,
            offset=offset,
            limit=limit,
            time_period=time_period
        )
        # TODO: Should we format json for raw string?
        save_json_with_dbutils(data, full_save_path, True, 2)
        if endpoint_config.paginable:
            logger.info(f"Saved endpoint: {endpoint.value} offset={offset} limit={limit} path={full_save_path}")
        else:
            logger.info(f"Saved endpoint: {endpoint.value} path={full_save_path}")                    
        return data, failed_offsets

    except Exception as e:
        # TODO: Check before paginable or not and type of error
        logger.exception(f"Request failed endpoint={endpoint.value} offset={offset} limit={limit} error: {e}")
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
        limit=20,
        time_period=None):
    
    # logger = get_logger(__name__)
    logger.info(f"Start retrieving data. Endpoint: {endpoint.value}")
    if query_params:
        query_params = query_params.copy() 
    else:
        query_params = {}
    page = 1
    offset = 0
    total = None
    endpoint_config = get_endpoint_config(endpoint)
    if endpoint_config.paginable is False:
        offset = None
        limit = None
    failed_offsets = []

    while True:
        data, failed_offsets = get_split_and_save_request(endpoint, 
                                                    path_params=path_params,
                                                    query_params=query_params,
                                                    offset=offset,
                                                    limit=limit,
                                                    time_period=time_period,
                                                    failed_offsets=failed_offsets)
        # TODO: Check error?
        if endpoint_config.paginable is False:
            break
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

        # TODO: Delete. for testing
        # if page > 4:
        #     print("Failed offsets:", failed_offsets)
        #     break

        # TODO: Add loading until offset >= total or empty result
        if total and offset >= total:
            if failed_offsets:
                logger.warning(f"Failed offsets: {failed_offsets}. Endpoint: {endpoint.value} Sent to Kafka")
            break
    logger.info(f"Finish retrieving data. Endpoint: {endpoint.value}")
