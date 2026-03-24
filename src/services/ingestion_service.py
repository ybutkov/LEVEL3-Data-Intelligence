from src.app.clients import get_lufthansa_client
from src.config.config_properties import get_ConfigProperties
from src.config.endpoints import get_endpoint_config
from src.util.json_utils import get_value_by_path
from src.services.storage_service import save_json_with_dbutils
from src.app.logger import get_logger

from http import HTTPStatus
import datetime
import time
import json
import requests
import random


logger = get_logger(__name__)

RETRYABLE_STATUS_CODES = {HTTPStatus.TOO_MANY_REQUESTS,
                          HTTPStatus.INTERNAL_SERVER_ERROR,
                          HTTPStatus.BAD_GATEWAY,
                          HTTPStatus.SERVICE_UNAVAILABLE,
                          HTTPStatus.GATEWAY_TIMEOUT}
NON_RETRYABLE_STATUS_CODES = {HTTPStatus.BAD_REQUEST,
                               HTTPStatus.UNAUTHORIZED,
                               HTTPStatus.FORBIDDEN,
                               HTTPStatus.NOT_FOUND}


def _parse_json_response(response):
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise ValueError("Response is not valid JSON") from exc

    if not isinstance(payload, dict):
        raise ValueError("Top-level JSON must be an object")
    return payload


def _validate_response_shape(endpoint_config, payload):
    validation_path = getattr(endpoint_config, "validation_path", None)
    if not validation_path:
        return
    marker = get_value_by_path(payload, validation_path)
    if marker is None:
        raise ValueError(
            f"Validation path not found for endpoint={endpoint_config.key}: "
            f"{validation_path}"
        )


def fetch_data(url, query_params=None, max_retries=5, timeout=20):
    if not url:
        logger.error(f"Empty url: {url}")
        raise ValueError(f"Empty url: {url}")

    httpClient = get_lufthansa_client()
    base_delay = 1.0
    delay = base_delay
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"Request to {url} attempt #{attempt}")
            response = httpClient.get(
                url,
                params=query_params,
                timeout=timeout,
            )
            logger.fatal(f"response={response.status_code}")
            status_code = response.status_code
            if status_code in RETRYABLE_STATUS_CODES:
                last_error = RuntimeError(f"Retryable status code {status_code}")
                if attempt == max_retries:
                    logger.error(f"Max retries exceeded. Status: {status_code}")
                    raise last_error
                
                sleep_seconds = min(delay, 20.0) + random.uniform(0, 0.5)
                logger.warning(
                    f"Retryable status {status_code}. "
                    f"Attempt {attempt}/{max_retries}. Sleep {sleep_seconds:.2f}s"
                )
                time.sleep(sleep_seconds)
                delay *= 2.0
                continue
            
            if status_code in NON_RETRYABLE_STATUS_CODES:
                logger.error(f"Non-retryable status code {status_code}")
                # Let requests.raise_for_status() raise a requests.HTTPError
                response.raise_for_status()

            response.raise_for_status()
            return response

        except (requests.Timeout, requests.ConnectionError) as e:
            logger.fatal("{e}")
            last_error = RuntimeError(f"Connection error: {type(e).__name__}")
            if attempt == max_retries:
                logger.error(f"Connection failed after {max_retries} attempts: {e}")
                raise last_error
            
            sleep_seconds = min(delay, 20.0) + random.uniform(0, 0.5)
            logger.warning(
                f"Connection error: {type(e).__name__}. "
                f"Attempt {attempt}/{max_retries}. Sleeping {sleep_seconds:.2f}s"
            )
            time.sleep(sleep_seconds)
            delay *= 2.0
        
        except ValueError as e:
            logger.fatal(f"ValueError={e}")
            raise e
        
        except requests.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None
            logger.fatal(f"status_code={status_code}")
            if status_code in RETRYABLE_STATUS_CODES:
                last_error = RuntimeError(f"Retryable HTTP error {status_code}")
                if attempt == max_retries:
                    logger.error(f"Max retries exceeded on HTTP error {status_code}")
                    raise last_error

                sleep_seconds = min(delay, 20.0) + random.uniform(0, 0.5)
                logger.warning(
                    f"Retryable HTTP error {status_code}. "
                    f"Attempt {attempt}/{max_retries}. Sleeping {sleep_seconds:.2f}s"
                )
                time.sleep(sleep_seconds)
                delay *= 2.0
            else:
                logger.error(f"Non-retryable HTTP error {status_code}")
                # Re-raise the original HTTPError so callers can inspect status_code
                raise e


def get_split_and_save_request(
        endpoint,
        path_params=None,
        query_params=None,
        offset=None,
        limit=None,
        time_period=None,
        failed_offsets=None):
    
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
        
        url = endpoint_config.build_endpoint_path(path_params)
        response = fetch_data(
            url=url,
            query_params=page_query_params,
        )
        logger.critical(f"After fetch, status_code={response.status_code}")
        data = _parse_json_response(response)
        _validate_response_shape(endpoint_config, data)
        logger.critical("2")
        if not endpoint_config.is_valid_response(data):
            raise ValueError("Fetching Error: response structure is not valid!!")
        logger.critical("3")
        full_save_path = endpoint_config.build_full_file_name(
            configProperties=configProperties,
            path_params=path_params,
            offset=offset,
            limit=limit,
            time_period=time_period
        )
        save_json_with_dbutils(data, full_save_path, True, 2)
        
        # if endpoint_config.paginable:
        #     logger.debug(f"Saved endpoint: {endpoint.value} offset={offset} limit={limit} path={full_save_path}")
        # else:
        #     logger.debug(f"Saved endpoint: {endpoint.value} path={full_save_path}")
        
        return data, failed_offsets

    except Exception as e:
        # TODO: Check before paginable or not and type of error
        logger.exception(f"Request failed endpoint={endpoint.value} offset={offset} limit={limit} error: {e}")

        # If this is an HTTP error, decide whether it's fatal (auth) or per-offset
        if isinstance(e, requests.HTTPError):
            status = e.response.status_code if getattr(e, "response", None) is not None else None
            # Treat auth errors as fatal for the whole run
            if status in {HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN}:
                raise e

            # If endpoint explicitly requests to stop on 404, honor it
            if status == HTTPStatus.NOT_FOUND and getattr(endpoint_config, "stop_on_404", False):
                logger.error(f"Endpoint configured to stop on 404: {endpoint.value}")
                raise e

            # For other resource-level errors (400/404 when not flagged), treat as per-offset failure
            logger.warning(
                f"HTTP error {status} for endpoint={endpoint.value} offset={offset}; "
                "marking offset or splitting range"
            )
            if not limit or limit <= 1:
                if offset is not None:
                    failed_offsets.append(offset)
                return data, failed_offsets
            # otherwise fall through to split-and-retry logic below

        if isinstance(e, ValueError):
            logger.warning(f"Parsing/validation error for endpoint={endpoint.value} offset={offset}: {e}")
            if not limit or limit <= 1:
                if offset is not None:
                    failed_offsets.append(offset)
                return data, failed_offsets

        # Split and retry recursively for retryable/unknown errors
        left_part = limit // 2
        right_part = limit - left_part

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

    # logger.info(f"Start retrieving data. Endpoint: {endpoint.value}")
    
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
        data, failed_offsets = get_split_and_save_request(
            endpoint,
            path_params=path_params,
            query_params=query_params,
            offset=offset,
            limit=limit,
            time_period=time_period,
            failed_offsets=failed_offsets
        )
        
        if endpoint_config.paginable is False:
            break

        if total is None:
            total = get_value_by_path(data, endpoint_config.total_count_path)
        
        if total is None:
            logger.error(f"Response does not contain 'total' for endpoint: {endpoint.value}")

        offset += limit
        page += 1

        if total and offset >= total:
            if failed_offsets:
                logger.warning(
                    f"Failed offsets: {failed_offsets}. "
                    f"Endpoint: {endpoint.value} - marking for retry"
                )
            break
    
    # logger.info(f"Finish retrieving data. Endpoint: {endpoint.value}")
