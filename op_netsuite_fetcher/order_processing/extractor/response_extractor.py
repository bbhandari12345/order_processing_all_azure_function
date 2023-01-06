from op_netsuite_fetcher.common.helpers import (
    split_if_multi_invoice_else_remove_duplicates_from_certain_keys,
    json_flattener, get_mapped_order_status,
    calculate_amount_and_total_if_not_exists,
    check_quantity_diff_and_set_status,
    check_shipcost_and_extraitemprice,
    split_single_object_to_multiple,
    map_object_with_config,
    prepare_config_files,
    make_api_call,
    carrier_mapper
)
from op_netsuite_fetcher.base_class import BaseExtractor
from op_netsuite_fetcher.constants import CANCELLED
from flatten_dict import flatten
from typing import Any, Dict
import xmltodict
import copy
import json


class OrderTransformer(BaseExtractor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._sonumber = None
        self.is_serialized = None
        self.mapped_order_details = []

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

        field_mapping_list = self.config_template.get('mapping').get('fulfillment_table')
        self.field_mapping = {
            fld_map.get('destination_field'): fld_map.get('source_field')
            for fld_map in field_mapping_list
        }
        return self

    def read_fetched_data(self) -> Any:
        self.logger.debug("Reading fetcher data")
        self.read_data()
        return self

    def multi_field_handler(self) -> Any:
        for obj in self.data:
            self._sonumber = obj.get("__sales_order_number__")
            self.is_serialized = obj.get("__need_serial_number__")

            ## Check if check_multi_field exists in config file. IF exists check if value of that field is in our
            ## response. If exists then iterate over that field and do extraction else only extract              
            if self.config_template.get("multi_field"):
                json_text_flat = flatten(
                    obj,
                    reducer="dot",
                    max_flatten_depth=len(self.config_template.get("multi_field").split("."))
                )

            if (
                self.config_template.get("multi_field")
                and json_text_flat
                and json_text_flat.get(self.config_template.get("multi_field"))
            ):
                if isinstance(json_text_flat.get(self.config_template.get("multi_field")), list):
                    self.is_multi_body = True
                    for obj in json_text_flat.get(self.config_template.get("multi_field")):
                        self.data = obj
                        self.transformer()
                else:
                    self.data = json_text_flat.get(self.config_template.get("multi_field"))
                    self.transformer()
            else:
                self.data = obj
                self.transformer()

            if not self.data:
                continue

            self.data.update({'so_number': self._sonumber})
            self.mapped_order_details.append(self.data)

        self.data = self.mapped_order_details
        return self

    def transformer(self) -> Any:
        self.logger.info("Deep flattening response")
        flat_response = json_flattener(self.data)
        
        self.logger.info("Mapping objects from response to config file")

        self.logger.info("Transforming sonumber %s of vendor %s" % (
            self._sonumber,
            self.kwargs.get('vendor_id'))
        )
        try:
            required_flattened = map_object_with_config(self, flat_response)
        except Exception as e:
            self.logger.error(
                'Error occurred during mapping of response for sonumber %s' 
                % self._sonumber
            )
            self.logger.error(e, exc_info=True)
            return
        
        ## Add sonumber and ponumber in required_flattened if not exists
        required_flattened.setdefault('invoice_number', required_flattened.get('tran_id'))
        required_flattened.setdefault('ship_date', required_flattened.get('tran_date'))

        if required_flattened.get("invoice_status"):
            _mapped_status = get_mapped_order_status(
                self.config_template.get("order_status_mapping"),
                required_flattened.get("invoice_status")
            )
            required_flattened.update({
                "invoice_status": _mapped_status
            })
        else:
            self.logger.info("Status Vendor not found. Moving onto next statement")

            if self.config_template.get(
                "order_status_in_different_field_and_deliveries_field_factor"
            ):
                self.logger.info('Checking for invoice field if it might have order_status as value. Specifically for DandH vendor')

                invoice_found_in_order_status_mapping = False
                if required_flattened.get('invoice_number'):
                    for k, v in self.config_template.get(
                        "order_status_mapping"
                    ).items():
                        if required_flattened.get('invoice_number') in v:
                            invoice_found_in_order_status_mapping = True
                            required_flattened.update({"invoice_status": k})
                            required_flattened.update({
                                "invoice_status_raw": required_flattened.get('invoice_number')
                            })
                            del required_flattened['invoice_number']

                    if (
                        not invoice_found_in_order_status_mapping 
                        and required_flattened.get('trackingnumber')
                    ):
                        required_flattened.update({"invoice_status": "COMPLETED"})
                        required_flattened.update({
                            "invoice_status_raw": required_flattened.get('invoice_number')
                        })

        if (
            self.config_template.get("multi_api_call") and
            self.config_template.get("multi_api_call").get("url")
            and required_flattened.get('invoice_status') != CANCELLED
            and bool(required_flattened.get('invoice_number'))
        ):
            if self.config_template.get("iterate_over_invoice_for_second_api_call"):
                for inv in required_flattened.get('invoice_number').split(','):
                    second_call_resp = self.make_multi_api_call(required_flattened, inv)
                    if second_call_resp == False:
                        continue
            else:
                second_call_resp = self.make_multi_api_call(
                    required_flattened,
                    required_flattened.get('invoice_number')
                )

        elif (
            required_flattened.get('invoice_status') != CANCELLED
            and bool(required_flattened.get('invoice_number'))
        ):
            self.data = required_flattened
            self.transform_data_further(
                invoice_number=required_flattened.get('invoice_number')
            )

        else:
            self.data = {}
            self.logger.info(
                "Order status is cancelled OR has no invoice number. Skipping this order"
            )

        return self

    def make_multi_api_call(self, order_obj: Dict, invoice_no: str = None) -> bool:
        temp_config_template = copy.deepcopy(self.config_template)

        template_str = json.dumps(temp_config_template)
        self.logger.info("Replacing template values for API request")

        # template_str = template_str.replace(f"{'<<invoice_number>>'}", invoice_no)
        # template_str = template_str.replace(f"{'<<so_number>>'}", self._sonumber)
        order_obj.setdefault('so_number', self._sonumber)

        for k,v in order_obj.items():
            if (
                self.config_template.get("data")
                and self.config_template.get("data").get("payload_value_to_upper")
                and k in self.config_template.get("data").get("payload_value_to_upper")
            ):
                v = str(v).upper()
            template_str = template_str.replace(f'<<{k}>>', str(v))

        loaded_template_str = json.loads(template_str)

        method = loaded_template_str.get("multi_api_call").get("url").get('method')
        url = loaded_template_str.get("multi_api_call").get("url").get("raw")
        headers = { item.get("key"):item.get("value") for item in self.config_template.get("multi_api_call").get("header") }
        data = loaded_template_str.get("data").get("xml_payload_second") if loaded_template_str.get("data") else None

        if loaded_template_str.get("data").get("x-www-form-urlencoded"):
            data = loaded_template_str.get("data").get("x-www-form-urlencoded")

        response = make_api_call(
            url=url,
            data=data,
            method=method,
            header=headers
        )

        if response.status_code in range(200, 210) and len(response.text) > 0:
            try:
                response_content = response.text
                if self.kwargs.get("vendor_type") == "xml":
                    response_content = json.dumps(xmltodict.parse(response_content))

                self.logger.info("Parsing xml response to python dictionary")
                deserialized_response = json.loads(response_content)
            except Exception as e:
                self.logger.error(
                    "Error during deserialization of response from vendor",
                    exc_info=True
                )
                raise Exception(e)

            flat_response = json_flattener(deserialized_response)

            try:
                mapped_object = map_object_with_config(self, flat_response)
            except Exception as e:
                self.logger.info(
                    'Error occurred during mapping of response for sonumber %s' 
                    % self._sonumber
                )
                self.logger.error(e, exc_info=True)
                return
            
            if loaded_template_str.get('use_items_from_first_api_call'):
                mapped_object.update({'items': order_obj.get('items')})

            mapped_object.update({
                'invoice_status': order_obj.get('invoice_status')
            })
            self.data = mapped_object
            self.transform_data_further(invoice_number=invoice_no)
        else:
            self.logger.info(
                "Response from vendor is not valid. Moving onto next invoice number"
            )
            return

        return self

    def transform_data_further(self, invoice_number):
        self.logger.info("Setting value of invoice")
        self.data.update({'invoice_number': invoice_number})

        if self.config_template.get("check_multi_invoice_and_split"):
            no_of_invoices = len(self.data.get('invoice_number').split(',')) if self.data.get('invoice_number') and len(self.data.get('invoice_number')) > 0 else 0
            try:
                if no_of_invoices > 1:
                    final_object = split_single_object_to_multiple(
                        self,
                        copy.deepcopy(self.data),
                        no_of_invoices,
                        self.config_template.get(
                            "check_for_tracking_number_and_set_invoice_accordingly"
                        )
                    )

                    if final_object:
                        self.data = copy.deepcopy(final_object)
            except Exception as e:
                self.data = {}
                self.logger.error(e, exc_info=True)
                return

        # Set status of each line items and overall order status
        # These status goes to vb_invoice table as a status of an order
        order_invoice_status, order_status_from_calculation = check_quantity_diff_and_set_status(self.data)
        if not order_invoice_status:
            self.data.update({
                'invoice_status': order_status_from_calculation
            })

        if self.data.get("deliveries"):
            #### Map carrier and carriermethod according to predefined mappers
            self.data = carrier_mapper(self, copy.deepcopy(self.data))

            ### Check for weight in deliveries. If weight is Zero or None then add 0.1
            for item in self.data.get("deliveries"):
                if not item.get('weight'):
                    item.update({'weight': '0.1'})

        # For Synnex use deliveries shipquantity value in items quantity
        if self.config_template.get("is_shipquantity_repeated"):
            for idx, obj in enumerate(self.data.get("deliveries")):
                if self.data.get("items"):
                    try:
                        self.data.get("items")[idx].update({
                            "quantity": obj.get("shipquantity")
                        })
                    except Exception as e:
                        break

        # Remove multiple value of cost in a single string
        split_if_multi_invoice_else_remove_duplicates_from_certain_keys(self.data)

        # Fill shicost and extraitemprice is not exists
        check_shipcost_and_extraitemprice(self)

        # Calculate amount for each item and total amount
        calculate_amount_and_total_if_not_exists(self.data)

        return self
