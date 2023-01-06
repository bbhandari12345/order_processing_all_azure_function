from op_netsuite_fetcher.conf import get_logger
from typing import Dict, Union, List, Any
from datetime import date, datetime
import re


logger = get_logger()


def check_shipcost_and_extraitemprice(self: Any):
    """
    Checks for shipcost and extraitemprice in an object and sets value accordingly
    """

    if (
        isinstance(self.data, dict) 
        and self.config_template.get('is_shipcost_and_extraitemprice_same')
    ):
        shipcost = self.data.get('ship_cost')
        extraitemprice  = self.data.get('extra_item_price')

        _shipcost = None if shipcost is None else str(shipcost)
        _extraitemprice = None if extraitemprice is None else str(extraitemprice)

        if not all([_shipcost, _extraitemprice]):
            shipcost, extraitemprice = shipcost if not _extraitemprice else extraitemprice, extraitemprice if not _shipcost else shipcost

            logger.info("Updating value of shipcost and extraitemprice")
            self.data.update({
                'ship_cost': shipcost,
                'extra_item_price': extraitemprice
            })
        else:
            logger.info("Both shipcost and extraitemprice has not null value. No need for update")
    else:
        logger.info("Shipcost and extraitemprice are different fields for mapping. So skipping this process")
    return self

def refactor_according_to_invoice(obj: Dict):
    if obj.get('invoice_number').find(',') == -1:
        logger.info("Taking single value of cost from multiple ones")
        # For cost taking maximum value
        for key in ('ship_cost', 'total', 'extra_item_price'):
            if not obj.get(key):
                continue
            
            if key == 'total':
                obj.update({'raw_total': obj.get(key)})
            val = max(re.findall('\d+\,*\d*\.\d+', obj.get(key)))
            obj.update({
                key: float(re.sub(r"[^\d\.\-]", "", val))
            })

        # For dates diff of date with current date and taking smallest value
        # could be done. But not sure whether that will satisfy our condition.
        # So taking first value. 
        logger.info("Taking single value of date from multiple dates")
        for key in ('ship_date', 'tran_date'):
            if not obj.get(key):
                continue
            obj.update({
                key: obj.get(key).split(',')[0]
            })
    else:
        logger.info('mulitiple invoice found. Skipping this part')
    return

def split_if_multi_invoice_else_remove_duplicates_from_certain_keys(data: Union[Dict, List]) -> Union[Dict, List]:
    if isinstance(data, dict):
        refactor_according_to_invoice(data)
    else:
        for obj in data:
           refactor_according_to_invoice(obj) 
    return data


def total_and_amount_calculation(obj: Dict) -> Dict:

    def get_units(obj: Dict, key: str, val: str) -> obj:
        """
        Return units by removing commas if any
        """
        val = re.sub(r"[^\d\.\-]", "", str(val))
        obj.update({
            key: val if key != 'quantity' else float(val)
        })
        return val

    # Get items from given obj
    item_list = obj.get('items')

    sum_of_amount = 0
    for item in item_list:
        if not str(item.get('rate')) or not str(item.get('quantity')):
            continue

        rate = get_units(item, 'rate', item.get('rate'))
        qty = get_units(item, 'quantity', item.get('quantity'))
        amount = float(rate) * float(qty)

        if not item.get('amount'):
            item.update({'amount': amount})
        sum_of_amount += amount if not item.get('amount') else float(item.get('amount'))

    if not obj.get('total'):
        obj.update({
            'total': float(sum_of_amount) + float(obj.get('ship_cost'))
        })

    return obj


def calculate_amount_and_total_if_not_exists(data: Union[Dict, List]) -> Union[Dict, List]:
    if isinstance(data, list):
        for _obj in data:
            total_and_amount_calculation(_obj)
    else:
        total_and_amount_calculation(data)

    return data


def get_index_from_string(key: str, find_idx_position: int=None) -> int:
    """
    Returns position of digits in a string if any

    :params key: key where any digit is searched
    :type key: str

    :returns: index/position of digit found in given key if not 0 is returned
    :rtype: int
    """
    idx_position = re.search("\.\d", key)
    
    idx = 0
    if idx_position != None and find_idx_position and len(key.split('.')) > find_idx_position:
        _idx = key.split('.')[find_idx_position]
        if _idx.isdigit():
            idx = int(_idx)
    elif idx_position != None:
        # idx_position.start() gives position of dot, plus one gives position of digit
        idx = int(key[idx_position.start()+1])
    return idx


def parse_datetime_format(obj: str, format_type: str) -> date:
    """
    Parse date time formats to a specific format

    :param obj: different datetime format obtained from vendor
    :type obj: string

    :param format_type: type of datetime format (eg: type 1,2,3)
    :type format_type: int

    :return: date or datetime to YYYY-MM-DD format
    :rtype: datetime
    """

    parsed_obj = obj
    if format_type:
        parsed_obj = datetime.strptime(obj.strip(' '), format_type).date()

    return str(parsed_obj)


def get_mapped_order_status(obj: Dict, field: str) -> str:
    """
    Returns Mapped status according to provided dictionary

    :params obj: Dictionary of mapped order status for list of status
    :type obj: dict

    :params field: order status to search in given object
    :type field: str

    :returns: matched mapped order status
    :type: str
    """

    for k,v in obj.items():
        if field in v:
            return k
    return field


def map_carrier_and_service(carrier: str, service: str, obj: Dict) -> int:
    """
    Maps carrier and service from response to given static value for respective vendors
    :params carrier: type of carrier used for shipping by a vendor in format ups or nonups
    :type carrier: str
    :params service: service used for shipping by a vendor that is to be mapped
    :type service: str
    :params obj: pre-defined mappers for service according to vendors
    :type obj: Dict
    :params is_lower: defines if service is to be changed to lowercase or not
    :type is_lower: boolean
    :return: Mapping id
    :type return: int
    """

    ShipMethodID = 0

    for k,v in obj.get(carrier).items():
        if k == service:
            ShipMethodID = v
            break

    return ShipMethodID
