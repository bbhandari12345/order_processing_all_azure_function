from op_dispatcher.constants import AZURE_STORAGE_CONNECTION_STRING
from azure.storage.blob import BlobServiceClient
from op_dispatcher.conf import get_logger
from typing import Union


logger = get_logger()


def connect_blob() -> Union[None, BlobServiceClient]:
    blob_service_client = None

    try:
        conn_str = AZURE_STORAGE_CONNECTION_STRING
        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str
        )
    except Exception as e:
        logger.error(e, exc_info=True)

    return blob_service_client
