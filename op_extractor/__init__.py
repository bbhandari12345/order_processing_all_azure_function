import json
import logging
from op_extractor.order_processing.extractor.response_extractor import OrderTransformer
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    obj = req.get_json()
    try:
        extractor = OrderTransformer(
            vendor_id=obj.get('vendor_id'),
            vendor_type=obj.get('vendor_type'),
            extractor_write_path=obj.get('extractor_write_path'),
            template_values=obj.get('template_values'),
            config_file_path=obj.get('config_file_path'),
            fetcher_file_path=obj.get('fetcher_file_path')
        ).execute()
        data = {
            "vendor_id": obj.get('vendor_id'),
            "template_values": obj.get('template_values'),
            "config_file_path": obj.get('config_file_path'),
            "extractor_file_path": extractor.meta['extractor_data_file_path']
           }
    except Exception as exe:
        logging.error(exe, exc_info=True)
        return func.HttpResponse("", status_code=500)
    return func.HttpResponse(json.dumps(data), status_code=200)

