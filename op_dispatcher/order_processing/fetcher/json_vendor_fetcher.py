from op_dispatcher.common.helpers import (
    make_api_call,
    prepare_config_files,
    get_mapped_order_status
)
from op_dispatcher.common.orm_handler.common_orm import execute_sql_query
from op_dispatcher.sql_queries import (
    QUERY_SELECT_SALESORDER
)
from op_dispatcher.constants import (
    PURCHASE_ORDER_STATUS_FOR_VENDOR_BILLING
)
from op_dispatcher.base_class import BaseFetcher
from typing import Any
import json


class JsonVendorOrderFetcher(BaseFetcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.required_template = None
        self.order_detail_list = []
        self.is_serialized = None

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

    def api_response_not_valid(self, flatten_response, response):
        error_obj = {}
        error_obj.update({
            "status_from_vendor": "ERROR",
            "vendor_message": response.reason
        })

        # error mapped to a status_vendor and vendor_message in db table to prevent re-ping to vendor for that order
        if self.required_template.get("error_mapping"):
            self.logger.info("Trying to map error from error_mapping field in vendor config file")

            error_status = flatten_response.get(
                self.required_template.get("error_mapping").get("error_status")
            )
            _mapped_status = get_mapped_order_status(
                self.required_template.get("order_status_mapping"),
                error_status
            )

            error_obj.update({
                "status_from_vendor": _mapped_status if _mapped_status else "ERROR"
            })

            if flatten_response.get(self.required_template.get("error_mapping").get("error_message")):
                error_obj.update({
                    "vendor_message": flatten_response.get(
                        self.required_template.get("error_mapping").get("error_message"))
                })
            else:
                self.logger.info("Vendor Message Couldnot be updated for error mapping")
        else:
            self.logger.info("Field to map error not found in vendor config file. Moving onto next statement")

            ## fault error mapped to a status_vendor and vendor_message in db table to prevent re-ping to vendor for that order 
            if self.required_template.get("fault_error_mapping"):
                self.logger.info("Trying to map error from fault_error_mapping field in vendor config file")

                error_status = flatten_response.get(
                    self.required_template.get("fault_error_mapping").get("fault_error_status")
                )
                _mapped_status = get_mapped_order_status(
                    self.required_template.get("order_status_mapping"),
                    error_status
                )

                error_obj.update({
                    "status_from_vendor": _mapped_status if _mapped_status else "ERROR"
                })

                if flatten_response.get(self.required_template.get("fault_error_mapping").get("fault_error_message")):
                    error_obj.update({
                        "vendor_message": flatten_response.get(
                            self.required_template.get("fault_error_mapping").get("fault_error_message"))
                    })
                else:
                    self.logger.info("Vendor Message Couldnot be updated for fault error mapping")
            else:
                self.logger.info("Field to map fault error not found in vendor config file. Moving onto next statement")

        return error_obj

    def extractor(self) -> Any:
        # Fetch list of sales orders of a vendor for calling vendor api
        sql = QUERY_SELECT_SALESORDER % (
            self.kwargs.get('vendor_id'),
            PURCHASE_ORDER_STATUS_FOR_VENDOR_BILLING
        )
        queryset = execute_sql_query(sql)

        headers = {item.get("key"): item.get("value") for item in
                   self.config_template.get("api_request_template").get("header")}
        for so in queryset.to_list():
            self.logger.debug(
                "Processing for vendor {%s} and sonumber {%s}" % (self.kwargs.get('vendor_id'), so.get('so_number')))
            self.logger.info(
                "Processing for vendor {%s} and sonumber {%s}" % (self.kwargs.get('vendor_id'), so.get('so_number')))

            self.is_serialized = so.get('need_serial_no')
            del so['need_serial_no']

            template_str = json.dumps(self.config_template)
            self.logger.info("Replacing template values for API request")

            for k, v in so.items():
                if (
                        self.config_template.get("data")
                        and self.config_template.get("data").get("payload_value_to_upper")
                        and k in self.config_template.get("data").get("payload_value_to_upper")
                ):
                    v = str(v).upper()
                template_str = template_str.replace(f'<<{k}>>', v)

            self.required_template = json.loads(template_str)

            self.logger.info("Sending request to vendor for order status")
            method = self.required_template.get("api_request_template").get("url").get("method")
            url = self.required_template.get("api_request_template").get("url").get("raw")
            response = make_api_call(method=method, header=headers, url=url)

            if len(response.text) == 0:
                self.logger.error("Response from vendor has no content")
                self.logger.error(response.reason, exc_info=True)
                continue
            else:
                try:
                    response_deserialize = json.loads(response.text)
                except Exception as e:
                    self.logger.error("Error during deserialization of response from vendor", exc_info=True)
                    raise Exception(e)

            if response.status_code not in range(200, 210):
                self.logger.error("The response from an API is not valid")
                self.logger.error(
                    "Could not get data from an API. API returned HTTP Status code: {}".format(response.status_code),
                    exc_info=True)
                continue
            else:
                response_deserialize.update({
                    "__need_serial_number__": self.is_serialized,
                    "__sales_order_number__": so.get('so_number')
                })
                self.order_detail_list.append(response_deserialize)

        self.data = self.order_detail_list
        return self
