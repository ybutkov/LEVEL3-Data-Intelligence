import config.config as cfg
from utils.ingestion_utils import *
import datetime

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

    offset = 10900
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