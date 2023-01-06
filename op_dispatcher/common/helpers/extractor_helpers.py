from typing import Dict, List, Union, Any
from op_dispatcher.conf import get_logger
import xmltodict
import requests
import json


logger = get_logger()


def data_to_be_inserted_into_table(
    data: List,
    invoice_list: Dict
) -> Union[List, Dict]:
    """
    Filters those objects that are not in given list of objects

    :param data:
    :type data:

    :param invoice_list: 
    :type invoice_list:

    :return:
    :type:
    """

    tmp_data_list = []
    for item in data:
        if item.get('invoice_number') and item.get('invoice_number') in invoice_list:
            continue
        elif item.get('invoice_number') and not invoice_list:
            tmp_data_list.append(item)
        elif not item.get('invoice_number'):
            tmp_data_list.append(item)

    return tmp_data_list


def prepare_config_files(self):
    # Read the config template
    self.read_config()

    # Load other values
    template_values = self.kwargs.get('template_values')
    template_str = json.dumps(self.config_template)

    logger.info("Replacing template values")
    for k, v in template_values.items():
        if (
            self.config_template.get("data")
            and self.config_template.get("data").get(
                "payload_value_to_upper"
            )
            and k in self.config_template.get("data").get(
                "payload_value_to_upper"
            )
        ):
            v = str(v).upper()
        template_str = template_str.replace(f'<<{k}>>', v)

    return template_str


def xml_to_json_parser(response: requests.Response) -> Any:
    logger.info("Parsing xml response to python dictionary")
    try:
        xml_text = json.dumps(xmltodict.parse(response.text))
        response_deserialize = json.loads(xml_text)
    except Exception as e:
        logger.error(response.text)
        logger.error("Error during deserialization of response from vendor", exc_info=True)
        raise Exception(e)
    return response_deserialize
