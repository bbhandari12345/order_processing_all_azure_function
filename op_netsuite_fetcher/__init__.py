import logging
import azure.functions as func
from op_netsuite_fetcher.execute_netsuite_fetcher import main as netsuite_fetcher


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Calling Netsuite Fetcher')
        logging.info('Http Triggered Netsuite Fetcher....')
        netsuite_fetcher()
    except Exception as exe:
        logging.error(exe, exc_info=True)
        return func.HttpResponse("", status_code=500)
    return func.HttpResponse("", status_code=200)
