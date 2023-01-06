from op_netsuite_fetcher.common.helpers import make_api_call, create_oauth_authorization
from op_netsuite_fetcher.orm import VbSalesOrder, VbPurchaseOrder
from op_netsuite_fetcher.base_class import BaseNetsuiteFetcher
from collections import ChainMap
from op_netsuite_fetcher.sql_queries.queries import (
    QUERY_UPDATE_SALESORDER_NUMBER_BY_CHECKING_SUBSTR
)
from op_netsuite_fetcher.conf import get_logger
from op_netsuite_fetcher.constants import (
    SALES_ORDER_TABLE_CONFLICT_FIELDS,
    PURCHASE_ORDER_TABLE_CONFLICT_FIELDS
)
from typing import Any
import json


logger = get_logger()


class NetsuiteFetcher(BaseNetsuiteFetcher):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.order_mapper = None

    def fetch_config(self) -> Any:
        # Read the config template
        self.read_config()

        self.order_mapper = self.config_template.get('order_info_mapping')
        vendor = self.kwargs.get('vendor_name')
        template_str = json.dumps(self.config_template)

        # Load vendor name in endpoint
        logger.info("Replacing template values")
        template_str = template_str.replace(
            f"{'<<vendor>>'}", vendor if vendor else 'all'
        )
        self.request_config = json.loads(template_str)

        return self

    def extractor(self) -> Any:
        url, headers, req_method = create_oauth_authorization(
            self=self
        )
        response = make_api_call(
            url=url,
            header=headers,
            method=req_method
        )

        if (response.status_code not in range(200, 210)):
            logger.error(
                "Could not get data from token generator.\
                API returned HTTP Status code: {}".format(
                    response.status_code
                )
            )
            logger.error(
                "The reason for ERROR: {}".format(
                    response.text
                ), exc_info=True
            )
            raise Exception('Data not found. API returned error status code')
        else:
            logger.info("Data loaded for transformation")
            self.data = response.text

        return self


    def transformer(self) -> Any:
        logger.info("Checking if the response is valid JSON")

        try:
            tmp = json.loads(self.data)
            tmp_flat_data = tmp['data']
        except ValueError as ex:
            logger.error(
                "The response is not a valid JSON",
                exc_info=True
            )
            raise Exception("The received response is not a valid JSON ", ex)

        if self.order_mapper is None:
            raise Exception(
                "Mapper for netsuite response is missing. \
                Check config file for missing mappings"
            )
        
        tmp_data = []
        _ = [tmp_data.append(x) for x in tmp_flat_data if f"{x.get('internalid')}, {x.get('createdfrom')}" not in [f"{y.get('internalid')}, {y.get('createdfrom')}" for y in tmp_data]]

        order_object_list = []
        for obj in tmp_data:
            final_order = dict()
            for k, v in self.order_mapper.items():
                final_order.update({k: obj[v]})
            
            if final_order['purchase_order_status'] not in self.config_template.get("purchase_order_status"):
                logger.error("Unknown Purchase Order Status. Status of purchase order doesnot match with pre-defined status")
                logger.debug("The purchase order whose status doesnot match is %s" % final_order)
                continue
            order_object_list.append(final_order)
        self.data = order_object_list

        return self


    def loader(self) -> Any:
        logger.info("Dispatching data from request to Sales Order Table")

        sales_order_list = []
        _ = [sales_order_list.append(x) for x in self.data if f"{x.get('soint_id')}" not in [f"{y.get('soint_id')}" for y in sales_order_list]]

        sales_order = VbSalesOrder(sales_order_list)
        sales_order.load()

        on_conflict_update_fields = 'sales_order_status=EXCLUDED.sales_order_status'
        sales_order_ids = sales_order.bulk_update_or_create(
            data=sales_order.loaded_data,
            conflict_fields=SALES_ORDER_TABLE_CONFLICT_FIELDS,
            on_conflict_update_fields=on_conflict_update_fields
        )

        sales_order_ids = dict(ChainMap(*sales_order_ids[0]['jsonb_agg']))

        for obj in self.data:
            obj["sales_order_id"] = sales_order_ids.get(obj["soint_id"])
            del obj["soint_id"]

        purchase_order = VbPurchaseOrder(self.data)
        purchase_order.load()

        on_conflict_update_fields = 'purchase_order_status=EXCLUDED.purchase_order_status'
        purchase_order_ids = purchase_order.bulk_update_or_create(
            data=purchase_order.loaded_data,
            conflict_fields=PURCHASE_ORDER_TABLE_CONFLICT_FIELDS,
            on_conflict_update_fields=on_conflict_update_fields
        )

        logger.info("Successfully dispatched data to Order table")
        return self


    def updater(self) -> Any:
        """
        Updates sonumber of Ingram if sonumber contains '-S' at last position
        """

        logger.info("Update SalesOrderNumber of vendor Ingram")

        sql = QUERY_UPDATE_SALESORDER_NUMBER_BY_CHECKING_SUBSTR % (36296)
        # execute_sql_query(sql)

        return self
