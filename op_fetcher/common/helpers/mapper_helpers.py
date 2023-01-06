from op_fetcher.constants import SKIPPING_CONSTANTS, SYMBOLS_IN_TECH_DATA_MAPPINGS
from op_fetcher.common.helpers.common_helpers import get_index_from_string
from typing import Dict
import re


def first_type_mapper(
    self,
    fld: str,
    item: Dict,
    tmp_dict: Dict
) -> None:
    """
    Map field present in response body, and column in table is not Dict/JSON
    """
    if self.field_mapping[fld].find('.') == -1:
        tmp_dict[self.field_mapping[fld]] = item.get(fld)
    else:
        field_map_first = self.field_mapping[fld].split(".")[0]
        field_map_second = self.field_mapping[fld].split(".")[-1]

        tmp_dict.setdefault(field_map_first, [{}])

        if (
            tmp_dict.get(field_map_first)
            and field_map_second != 'item_details'
        ):
            tmp_dict[field_map_first][0].update({
                field_map_second: item.get(fld)
            })

        if (
            field_map_first == "items"
            and "items" in tmp_dict
            and "item_details" not in tmp_dict["items"][0]
        ):
            tmp_dict[field_map_first][0].update({
                "item_details": []
            })

        if (
            field_map_second == "item_details"
            and item.get(fld)
            and self.is_serialized is not False
        ):
            tmp_dict[field_map_first][0][field_map_second].append({
                "serialnumber": item.get(fld)
            })

    return


def second_type_mapper(
    self,
    fld: str,
    item: Dict,
    tmp_dict: Dict
):
    """
    Mapping specific sign to it a certain field.
    Special case for Techdata.
    """

    sign = SYMBOLS_IN_TECH_DATA_MAPPINGS.get(fld)
    field_to_find = fld[:fld.find(f"['{sign}']")]
    for k, _ in item.items():
        if k.endswith(field_to_find) and item[k] == f'{sign}':
            req_fld = k[:-len(field_to_find)] + 'RefID'
            tmp_dict[self.field_mapping[fld]] = item.get(req_fld)
            break
    return


def check_is_last_index_digit_and_set_condition(
    self,
    key,
    key_words,
    field_to_endwith,
    field_to_startwith,
    is_last_index_digit
):
    """
    Sets condition for if statement on the basis of is_last_index_digit
    """

    # Separating condition for items with field_to_endwith having last index digit and not.
    key_first = key_words[0]
    key_last = key_words[-1]
    # If last index is digit then condition for if-statement will be according to second last index since our last index is digit
    if (key_last.isdigit() or re.search('[0-9]', key_last)) and len(key_words) > 1:
        key_last = key_words[-2]
    if self.config_template.get("field_to_start_with_split_again"):
        field_to_startwith = field_to_startwith.split('.')[0]
    if_statement_condition = (key_first == field_to_startwith and key_last == field_to_endwith)
    return if_statement_condition


def set_is_last_index_digit(
    field_to_endwith,
    field_to_startwith
) -> bool:
    """
    Sets value of is_last_index_digit
    """

    # Check if field_to_startwith and field_to_endwith has '[i]'.
    # If field_to_endwith has then last index will be digit
    is_last_index_digit = False
    if field_to_startwith.find('[i]') != -1:
        field_to_startwith = field_to_startwith[:field_to_startwith.find('[i]')]
    if field_to_endwith.find('[i]') != -1:
        field_to_endwith = field_to_endwith[:field_to_endwith.find('[i]')]
        is_last_index_digit = True
    return is_last_index_digit


def third_type_mapper(
    self,
    fld,
    item,
    tmp_dict,
    field_to_endwith,
    field_to_startwith
):
    """
    Maps those values whose key is a nested dict and target to be mapped in table is also a nested column
    eg:-
        {
            "serviceresponse.invoicedetailresponse.lines.0.partnumber": "KLM-5029"
        }

        will be mapped to

        {
            "items": [
                {
                    "itemno": "KLM-5029"
                }
            ]
        }
    """

    field_map_first = self.field_mapping[fld].split(".")[0]
    field_map_second = self.field_mapping[fld].split(".")[-1]

    is_last_index_digit = set_is_last_index_digit(
        field_to_endwith=field_to_endwith,
        field_to_startwith=field_to_startwith
    )

    tmp_dict.setdefault(field_map_first, [])

    for key, value in item.items():
        key_words = key.split('.')

        if_statement_condition = check_is_last_index_digit_and_set_condition(
            key=key,
            self=self,
            key_words=key_words,
            field_to_endwith=field_to_endwith,
            field_to_startwith=field_to_startwith,
            is_last_index_digit=is_last_index_digit
        )

        if if_statement_condition:
            if field_map_first == 'items':
                idx = get_index_from_string(
                    key,
                    self.config_template.get("position_for_checking_idx_for_nesting")
                )
            elif field_map_first == 'deliveries':
                idx = get_index_from_string(
                    key,
                    self.config_template.get("position_for_checking_idx_deliveries")
                )
            else:
                idx = get_index_from_string(key)

            if (
                self.config_template.get('make_items_inside_items')
                and
                self.config_template.get(
                    "make_items_inside_items"
                ).get('field_to_nest') == field_map_first
            ):
                nested_idx = get_index_from_string(
                    key,
                    self.config_template.get(
                        "position_for_checking_idx_for_nesting"
                    )
                )

                if len(tmp_dict[field_map_first]) >= (idx+1):
                    if len(tmp_dict[field_map_first][idx]) >= (nested_idx+1):
                        tmp_dict[field_map_first][idx][nested_idx].update({
                            field_map_second: value
                        })
                    else:
                        tmp_dict[field_map_first][idx].insert(
                            nested_idx,
                            {field_map_second: value}
                        )
                else:
                    tmp_dict[field_map_first].insert(idx, [])
                    tmp_dict[field_map_first][idx].insert(
                        nested_idx,
                        {field_map_second: value}
                    )

                if (
                    field_map_first == "items"
                    and "items" in tmp_dict
                    and len(tmp_dict["items"]) < (idx+1)
                ):
                    tmp_dict[field_map_first].insert(idx, [])
            else:
                if (
                    tmp_dict[field_map_first]
                    and len(tmp_dict[field_map_first]) >= (idx+1)
                ):
                    tmp_dict[field_map_first][idx].update({
                        field_map_second: value
                    })
                else:
                    tmp_dict[field_map_first].insert(
                        idx,
                        {field_map_second: value}
                    )
        else:
            pass
    return tmp_dict


def fourth_type_mapper(
    item,
    tmp_dict,
    field_map_first,
    field_map_second,
    field_to_startwith
):
    """

    """

    for k, v in item.items():
        if k.startswith(field_to_startwith):
            idx = get_index_from_string(k)
            tmp_dict.setdefault(field_map_first, [])

            if (
                tmp_dict.get(field_map_first)
                and len(tmp_dict[field_map_first]) >= (idx+1)
            ):
                tmp_dict[field_map_first][idx].update({
                    field_map_second: v
                })
            else:
                tmp_dict[field_map_first].insert(
                    idx,
                    {field_map_second: v}
                )
    return tmp_dict


def fifth_type_mapper(
    self,
    fld,
    item,
    tmp_dict,
    field_map_first,
    field_to_endwith,
    field_map_second,
    field_to_startwith,
    is_last_index_digit
):
    """

    """
    for key, value in item.items():
        key_words = key.split('.')

        if_statement_condition = check_is_last_index_digit_and_set_condition(
            key=key,
            self=self,
            key_words=key_words,
            field_to_endwith=field_to_endwith,
            field_to_startwith=field_to_startwith,
            is_last_index_digit=is_last_index_digit
        )

        if (
            len(self.field_mapping[fld].split(".")) > 1
            and if_statement_condition
        ):
            tmp_dict.setdefault(field_map_first, [])

            if len(key_words) > 1:
                if field_map_first == 'items':
                    idx = get_index_from_string(
                        key,
                        self.config_template.get("position_for_checking_idx_for_nesting")
                    )
                else:
                    idx = get_index_from_string(key)

                if (
                    tmp_dict[field_map_first]
                    and len(tmp_dict[field_map_first]) >= (idx+1)
                ):
                    tmp_dict[field_map_first][idx].update({
                        field_map_second: value
                    })
                else:
                    tmp_dict[field_map_first].insert(
                        idx,
                        {field_map_second: value}
                    )
    return tmp_dict


def map_object_with_config(self, item):
    """

    """
    tmp_dict = {}
    for fld in self.field_mapping.keys():
        if len(fld) == 0:
            continue

        if fld in item:
            if (
                self.field_mapping[fld] in SKIPPING_CONSTANTS
                and not item.get(fld)
            ):
                pass
            else:
                first_type_mapper(self, fld, item, tmp_dict)
            continue

        # SYMBOLS_IN_TECH_DATA_MAPPINGS
        # RefIDQual['IN'] & RefIDQual['ON'] --> For techdata
        if fld in SYMBOLS_IN_TECH_DATA_MAPPINGS.keys():
            second_type_mapper(self, fld, item, tmp_dict)

        if_fld_has_multi = fld.find('[i]')
        if if_fld_has_multi != -1:
            field_to_startwith = fld[:if_fld_has_multi]
            fields = fld.split(".")
            field_to_endwith = fields[-1]

            field_to_startwith = field_to_startwith.split('.')[0]
            field_to_endwith = field_to_endwith.split('.')[-1]

            if (
                len(fields) > 1
                and
                len(self.field_mapping[fld].split(".")) > 1
            ):
                third_type_mapper(
                    fld=fld,
                    self=self,
                    item=item,
                    tmp_dict=tmp_dict,
                    field_to_endwith=field_to_endwith,
                    field_to_startwith=field_to_startwith
                )
            else:
                if (
                    len(self.field_mapping[fld].split('.')) > 1
                    and len(fields) == 1
                ):
                    field_map_first = self.field_mapping[fld].split(".")[0]
                    field_map_second = self.field_mapping[fld].split(".")[-1]

                    fourth_type_mapper(
                        item=item,
                        tmp_dict=tmp_dict,
                        field_map_first=field_map_first,
                        field_map_second=field_map_second,
                        field_to_startwith=field_to_startwith
                    )
                else:
                    field_map_first = self.field_mapping[fld]
                    result = None
                    if field_to_endwith.find("[i]") != -1:
                        field_to_endwith = field_to_startwith
                        result = ", ".join(value for key, value in item.items() if key.startswith(field_to_endwith))
                    elif field_to_startwith.find("[i]") != -1:
                        field_to_startwith = field_to_endwith
                        result = ", ".join(value for key, value in item.items() if key.endswith(field_to_startwith))
                    elif field_map_first != "ship_date":
                        result = ", ".join(value for key, value in item.items() if key.startswith(field_to_startwith) and key.endswith(field_to_endwith))
                    else:
                        result = ", ".join(value for key, value in item.items() if value if key.startswith(field_to_startwith) and key.endswith(field_to_endwith))

                    if (
                        result is None
                        and field_map_first in SKIPPING_CONSTANTS
                    ):
                        continue

                    tmp_dict.update({field_map_first: result})

        else:
            fields = fld.split(".")
            field_to_startwith = fields[0]
            field_to_endwith = fields[-1]

            is_last_index_digit = set_is_last_index_digit(
                field_to_endwith=field_to_endwith,
                field_to_startwith=field_to_startwith
            )

            field_map_first = self.field_mapping[fld].split(".")[0]
            field_map_second = self.field_mapping[fld].split(".")[-1]

            fifth_type_mapper(
                fld=fld,
                self=self,
                item=item,
                tmp_dict=tmp_dict,
                field_map_first=field_map_first,
                field_to_endwith=field_to_endwith,
                field_map_second=field_map_second,
                field_to_startwith=field_to_startwith,
                is_last_index_digit=is_last_index_digit
            )
    return tmp_dict
