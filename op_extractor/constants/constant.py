from enum import Enum

CONFIG_FILE_PATH = "op_extractor/config.toml"
SALES_ORDER_TABLE_CONFLICT_FIELDS = "soint_id"
PURCHASE_ORDER_TABLE_CONFLICT_FIELDS = "point_id"

# For Techdata
DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'

# Order Status
OPEN = "OPEN"
BACKORDERED = "BACKORDERED"
PROCESSING = "PROCESSING"
PARTIAL = "PARTIAL"
COMPLETED = "COMPLETED"
CANCELLED = "CANCELLED"
ERROR = "ERROR"

# Mapping Constants
SKIPPING_CONSTANTS = ("memo", "shipdate", "invoice_status", "invoice_number")

# Techdata Constants
SYMBOLS_IN_TECH_DATA_MAPPINGS = {
    "RefIDQual['IN']": 'IN',
    "RefIDQual['ON']": 'ON'
}

PURCHASE_ORDER_STATUS_FOR_VENDOR_BILLING = (
'pendingReceipt', 'partiallyReceived', 'pendingBilling', 'pendingBillPartReceived')

BLOB_NAME = "BLOB_NAME"
CONFIG_DIRECTORY = "CONFIG_DIRECTORY"
BLOB_CONTAINER_NAME = "BLOB_CONTAINER_NAME"
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=savapi;AccountKey" \
                                  "=U7AF6GQLjl584h3HON2OQnA0YZIN86y32Jq3yRb1DvOGZ0nIm+1u6syEwaqsls01WRkMzpFmshyJ" \
                                  "+AStNWC0tQ==;EndpointSuffix=core.windows.net"

NETSUITE_ENV = {
    "Preprod": "preprod_raw",
    "Prod": "prod_raw"
}
OAUTH1_ENV = {
    "Preprod": "oauth1_preprod",
    "Prod": "oauth1_prod"
}
METHOD_URL = {
    "GET": "url",
    "POST": "post_url"
}


class ObjectType(Enum):
    FETCHER = "FETCHER"
    EXTRACTOR = "EXTRACTOR"
    DISPATCHER = "DISPATCHER"


class ConfigFields(Enum):
    ENVIRONMENT = "ENVIRONMENT"
    CONFIG_FILE_TYPE = "CONFIG_FILE_TYPE"
    VENDOR_CONFIG_PATH = "VENDOR_CONFIG_PATH"
    NETSUITE_CONFIG_PATH = "NETSUITE_CONFIG_PATH"
    FETCHER_WRITE_PATH = "FETCHER_WRITE_PATH"
    EXTRACTOR_WRITE_PATH = "EXTRACTOR_WRITE_PATH"
