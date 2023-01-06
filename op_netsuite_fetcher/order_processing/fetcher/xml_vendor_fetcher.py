from op_netsuite_fetcher.common.helpers import (
    make_api_call,
    xml_to_json_parser,
    prepare_config_files,
)
from op_netsuite_fetcher.common.orm_handler.common_orm import execute_sql_query
from op_netsuite_fetcher.base_class import BaseFetcher
from op_netsuite_fetcher.sql_queries import (
    QUERY_SELECT_PURCHASE_ORDER_AND_SALESORDER
)
from op_netsuite_fetcher.constants import (
    DEFAULT_CIPHERS,
    PURCHASE_ORDER_STATUS_FOR_VENDOR_BILLING
)
from flatten_dict import flatten
from typing import Any
import json


class XMLVendorOrderFetcher(BaseFetcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.is_serialized = None
        self.order_detail_list = []
        self.required_template = None

        # Check if all the mandatory parameters are present
        mandatory_params = ['vendor_id', 'config_file_path']
        for param in mandatory_params:
            if param not in kwargs:
                raise Exception("Mandatory parameter '{}' not found".format(param))

    def fetch_config(self) -> Any:
        """
        Read the config file template and construct final config object out of it
        """

        self.logger.info("Prepare config files for API calls")

        template_str = prepare_config_files(self)
        self.config_template = json.loads(template_str)
        return self

    def extractor(self) -> Any:
        # Fetch list of sales orders of a vendor for calling vendor api
        sql = QUERY_SELECT_PURCHASE_ORDER_AND_SALESORDER % (
            self.kwargs.get('vendor_id'),
            PURCHASE_ORDER_STATUS_FOR_VENDOR_BILLING
        )
        queryset = execute_sql_query(sql)

        headers = { item.get("key"):item.get("value") for item in self.config_template.get("api_request_template").get("header") }
        for po in queryset.to_list():
            self.logger.debug("Processing for vendor {%s} and ponumber {%s}" % (self.kwargs.get('vendor_id'), po.get("po_number")))
            self.logger.info("Processing for vendor {%s} and ponumber {%s}" % (self.kwargs.get('vendor_id'), po.get("po_number")))

            self.is_serialized = po.get("need_serial_no")
            del po['need_serial_no']

            template_str = json.dumps(self.config_template)
            self.logger.info("Replacing template values for API request")

            for k, v in po.items():
                if (
                    self.config_template.get("data")
                    and self.config_template.get("data").get("payload_value_to_upper")
                    and k in self.config_template.get("data").get("payload_value_to_upper")
                ):
                    v = str(v).upper()
                template_str = template_str.replace(f'<<{k}>>', str(v))

            self.required_template = json.loads(template_str)

            self.logger.info("Sending request to vendor for order status")
            method = self.required_template.get("api_request_template").get("url").get("method")
            url = self.required_template.get("api_request_template").get("url").get("raw")
            data = self.required_template.get("data").get("xml_payload") if method == 'POST' else None
            if self.required_template.get("data").get("x-www-form-urlencoded"):
                data = self.required_template.get("data").get("x-www-form-urlencoded")

            response = make_api_call(
                url=url,
                data=data,
                method=method,
                header=headers,
                default_ciphers=DEFAULT_CIPHERS
            )

            if len(response.text) == 0:
                self.logger.error("Response from vendor has no content")
                self.logger.error(response.reason, exc_info=True)
                continue

            if response.status_code not in range(200, 210):
                self.logger.error("The response from an API is not valid")
                self.logger.error("Could not get data from an API. API returned HTTP Status code: {}".format(response.status_code), exc_info=True)
                continue

            response_deserialize = xml_to_json_parser(response)

            if self.required_template.get("check_response_body"):
                flat_json = flatten(
                    response_deserialize,
                    reducer="dot",
                    max_flatten_depth=len(self.required_template.get("check_response_body").split("."))
                )
                if flat_json.get(self.required_template.get("check_response_body")) is None:
                    continue

            response_deserialize.update({
                "__need_serial_number__": self.is_serialized,
                "__sales_order_number__": po.get('so_number')
            })
            self.order_detail_list.append(response_deserialize)

        self.data = self.order_detail_list
        return self
