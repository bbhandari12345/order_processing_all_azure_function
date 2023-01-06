from op_extractor.common.helpers.common_helpers import map_carrier_and_service
from typing import Dict, List, Union, Tuple
from op_extractor.conf import get_logger
import copy
import re


logger = get_logger()


def carrier_mapper(self, data: Dict) -> Dict:
    """
    Maps carrier and carrier_method/service with respective given values that are in configuration
    file for given vendor
    :param self_obj: instance of a class
    :type self_obj:
    :param data: response from vendor mapped with respective to config file
    :type data: dict
    :return: carrier and service mapped data
    :type: dict
    """

    mapper_obj = dict()
    if self.config_template.get("carrier_mapping"):
        mapper_obj = self.config_template.get("carrier_mapping")
    else:
        logger.info("Carrier Mapping not found. Moving onto next statement")

    idx = 0
    for item in data.get("deliveries"):
        if item.get("carrier"):
            carrier_name = item.get("carrier").lower()

            if self.config_template.get("carrier_mapping").get("check_character_for_mapping"):
                if carrier_name[0] == 'u':
                    carrier_name = 'ups'
                else:
                    carrier_name = 'nonups'
            else:
                logger.info("Character Mapping is not used for this item. Moving onto next statement")

            if self.config_template.get("carrier_mapping").get("check_string_for_mapping"):
                if re.search("ups", carrier_name):
                    carrier_name = "ups"
                else:
                    carrier_name = "nonups"
            else:
                logger.info("String Mapping is not used for this item. Moving onto next statement")

            if self.config_template.get('carrier_mapping').get("carriermethod_for_mapping") == "carriermethod":
                CARRIER_SERVICE = "carriermethod"
            else:
                CARRIER_SERVICE = "carrier"

            if self.config_template.get("carrier_mapping").get("change_carriermethod_to_lowercase"):
                service_name = item.get(CARRIER_SERVICE).lower()
            else:
                service_name = item.get(CARRIER_SERVICE)

            service_int = map_carrier_and_service(carrier=carrier_name, service=service_name, obj=mapper_obj)
            data["deliveries"][idx].update({
                "carrier": carrier_name,
                "carriermethod": service_int
            })
        else:
            logger.info("Carrier not found in an order object. Checking next iteration item")

        idx += 1

    return data


def status_calculation(data: Dict) -> str:
    status = ""

    qt_ship = data.get('quantity')                ### Quantity Shipped
    qt_ord = data.get('quantity_ordered')         ### Quantity Ordered
    qt_back = data.get('quantity_backordered')    ### Quantity BackOrdered


    if not qt_back and qt_ord and qt_ship:
        if int(qt_ord) - int(qt_ship) == 0:
            status = "COMPLETED"
        elif int(qt_ord) - int(qt_ship) == int(qt_ord):
            status = "PROCESSING"
        elif int(qt_ord) - int(qt_ship) > 0 and int(qt_ord) - int(qt_ship) < int(qt_ord):
            status = "PARTIAL"
        else:
            pass

    elif qt_back and qt_ord and qt_ship:
        if int(qt_ord) == int(qt_back) and int(qt_ship) != int(qt_ord):
            status = "BACKORDERED"
        elif int(qt_ship) == int(qt_ord):
            status = "COMPLETED"
        elif int(qt_ord) - int(qt_ship) > 0 and int(qt_ord) - int(qt_ship) < int(qt_ord):
            status = "PARTIAL"
        else:
            pass

    elif not qt_back and qt_ord and not qt_ship:
        status = "PROCESSING"

    # This condition is used on the basis of Techdata response
    elif not qt_back and not qt_ord and qt_ship:
        status = 'COMPLETED'
    
    elif qt_ship and qt_back and not qt_ord:
        if int(qt_back) == 0 and int(qt_ship) > 0:
            status = "COMPLETED"
        elif int(qt_back) > 0 and int(qt_ship) == 0:
            status = "BACKORDERED"
        elif int(qt_ship) > 0 and int(qt_back) > 0:
            status = "PARTIAL"
        else:
            pass

    else:
        status = data.get('invoice_status')

    return status


def check_quantity_diff_and_set_status(data: Union[Dict, List]) -> Tuple[str, str]:
    """
    :param data:
    :type data: dict

    :return:
    :type: dict
    """

    def process_list_obj(_dict, order_status_from_calculation):
        status_list = []
        for item in _dict.get('items'):
            if isinstance(item, dict):
                status = status_calculation(item)
                if status:
                    status_list.append(status)
            else:
                for obj in item:
                    status = status_calculation(obj)
                    if status:
                        status_list.append(status)

        if status_list:
            if all(x == 'COMPLETED' for x in status_list):
                order_status_from_calculation = 'COMPLETED'
            if any(x == 'PROCESSING' for x in status_list):
                order_status_from_calculation = 'PROCESSING'
            if any(x == 'BACKORDERED' for x in status_list):
                order_status_from_calculation = 'PARTIAL'
            if any(x == 'PARTIAL' for x in status_list):
                order_status_from_calculation = 'PARTIAL'
            if all(x == 'BACKORDERED' for x in status_list):
                order_status_from_calculation = 'BACKORDERED'
        
        return order_status_from_calculation

    order_status_from_vendor = data.get('status_from_vendor')
    order_status_from_calculation = ""

    if isinstance(data, dict):
        if isinstance(data.get('items'), dict):
            status = status_calculation(data)
            order_status_from_calculation = status
        else:
            order_status_from_calculation = process_list_obj(data, order_status_from_calculation)
    else:
        for item in data:
            if isinstance(item.get('items'), dict):
                status = status_calculation(item)
                order_status_from_calculation = status
            else:
                order_status_from_calculation = process_list_obj(item, order_status_from_calculation)        

    return order_status_from_vendor, order_status_from_calculation


def split_single_object_to_multiple(self_obj, data: Dict, invoice_length: int, tracking_number_list: List = None) -> Union[List[Dict],List]:
    """
    Split single mapped data to multiple with respective to line items. If a single order object
    has multiple line items then they are splitted into respective numbers

    :param self_obj: instance of class
    :type self_obj: 

    :param data: order object
    :type data: dict

    :param invoice_length: length of invoice that are in type string
    :type invoice_length: int

    :param tracking_number_list: list of some tracking numbers in deliveries that doesnot have any respective invoices
    :type tracking_number_list: list

    :return: list of order objects after being splitted
    :type: dict
    """

    constant_object = {k: v for k, v in data.items() if k not in self_obj.config_template.get('fields_to_split_for_multi_invoice')}
    items_length = len(data.get('items')) if data.get('items') else 0

    final_object = []
    if invoice_length >= 1 and tracking_number_list is None and invoice_length == items_length:
        shipdate = {}
        items_by_invoice = {}

        for i in range(invoice_length):
            items_by_invoice[data.get('invoice_number').split(',')[i].strip(" ")] = data['items'][i]

            if data.get('ship_date') and len(data['ship_date']) > 0:
                shipdate[data.get('invoice_number').split(',')[i].strip(" ")] = data['ship_date'].split(',')[i]

        for k,v in items_by_invoice.items():
            if isinstance(v, list):
                for obj in v:
                    fo = copy.deepcopy(constant_object)
                    fo.update({
                        "invoice_number": k,
                        "items" : obj
                        })
                    if shipdate:
                        fo.update({"ship_date": shipdate[k]})
                    final_object.append(fo)
            else:
                fo = copy.deepcopy(constant_object)
                fo.update({
                    "invoice_number": k,
                    "items" : items_by_invoice[k]
                    })

                if shipdate:
                    fo.update({"ship_date": shipdate[k]})
                final_object.append(fo)

    elif items_length >=1 and invoice_length >=1 and items_length != invoice_length:
        item_length = len(data.get('items'))
        date_length = len(data.get("ship_date").split(','))
        range_length = max(len(data.get('items')), invoice_length)

        for i in range(range_length):
            temp_object = copy.deepcopy(constant_object)
            item = data.get('items')[i] if item_length > i else data.get('items')[item_length-1]
            shipdate = data.get('ship_date').split(',')[i] if date_length > i else data.get('ship_date').split(',')[date_length-1]
            invoice = data.get('invoice_number').split(',')[i] if invoice_length > i else data.get('invoice_number').split(',')[invoice_length-1]

            temp_object.update({
                'ship_date': shipdate,
                'invoice_number': invoice
            })

            if isinstance(item, list):
                for obj in item:
                    temp_object.update({'items': obj})
            else:
                temp_object.update({'items': item})

            final_object.append(temp_object)

    elif items_length >=1: 
        if data.get('items'):
            range_length = len(data.get('items'))
        else:
            range_length = None

        if range_length:
            final_object = [copy.deepcopy(constant_object) for _ in range(range_length)]

            for field in self_obj.config_template.get('fields_to_split_for_multi_invoice'):
                val = data.get(field)
                if val:
                    if isinstance(val, str):
                        val = val.split(',')
                    for idx, v in enumerate(val):
                        final_object[idx].update({field: v.strip(' ') if isinstance(v, str) else v})

    return final_object
