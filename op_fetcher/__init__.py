import logging
import json
from op_fetcher.constants import VENDOR_CONFIG_PATH, FETCHER_WRITE_PATH, EXTRACTOR_WRITE_PATH
from op_fetcher.order_processing.fetcher import JsonVendorOrderFetcher, XMLVendorOrderFetcher
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    obj = req.get_json()
    try:
        if obj.get('connection_type') == "api":
            fetcher = JsonVendorOrderFetcher(
                vendor_id=obj.get('vendor_id'),
                fetcher_write_path=FETCHER_WRITE_PATH,
                template_values=obj.get('template_values'),
                config_file_path=obj.get('config_file_path')
            ).execute()
        elif obj.get('connection_type') == "xml":
            fetcher = XMLVendorOrderFetcher(
                vendor_id=obj.get('vendor_id'),
                fetcher_write_path=FETCHER_WRITE_PATH,
                template_values=obj.get('template_values'),
                config_file_path=obj.get('config_file_path')
            ).execute()
        data = {
            "vendor_id": obj.get('vendor_id'),
            "vendor_type": obj.get('connection_type'),
            "extractor_write_path": EXTRACTOR_WRITE_PATH,
            "template_values": obj.get('template_values'),
            "config_file_path": obj.get('config_file_path'),
            "fetcher_file_path": fetcher.meta['fetcher_data_file_path']
        }
    except Exception as exe:
        logging.error(exe, exc_info=True)
        return func.HttpResponse("", status_code=500)
    return func.HttpResponse(json.dumps(data), status_code=200)
