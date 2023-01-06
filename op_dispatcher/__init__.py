import logging
from op_dispatcher.order_processing.dispatcher.load_to_db import OrderLoader
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    obj = req.get_json()
    try:
        dispatcher = OrderLoader(
            vendor_id=obj.get('vendor_id'),
            template_values=obj.get('template_values'),
            config_file_path=obj.get('config_file_path'),
            extractor_file_path=obj.get('extractor_file_path'),
        ).execute()
    except Exception as exe:
        logging.error(exe, exc_info=True)
        return func.HttpResponse("", status_code=500)
    return func.HttpResponse("", status_code=200)
