import logging
import json
from op_schedular.common.orm_handler.common_orm import execute_sql_query
import azure.functions as func
from op_schedular.sql_queries import QUERY_FETCH_VENDORS_INFORMATION
from op_schedular.constants import VENDOR_CONFIG_PATH, FETCHER_WRITE_PATH, EXTRACTOR_WRITE_PATH


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("----- Running Scheduler ------")

        vendor_list = execute_sql_query(
            QUERY_FETCH_VENDORS_INFORMATION,
            {'config_file_path': VENDOR_CONFIG_PATH}
        )
        vendor_list = [obj.get('jsonb_build_object') for obj in vendor_list.to_list()]

    except Exception as exe:
        logging.error(exe, exc_info=True)
        return func.HttpResponse("", status_code=500)
    return func.HttpResponse(json.dumps(vendor_list), status_code=200)
