from op_schedular.sql_queries import (
    QUERY_UPSERT_INTO_ORDER_BILL,
    QUERY_FETCH_INVOICE_NUMBER_FOR_ORDER,
    QUERY_SELECT_ORDER_ID_FOR_GIVEN_PO_NUMBER,
    QUERY_SELECT_ORDER_ID_FOR_GIVEN_SO_NUMBER,
)
from op_schedular.common.orm_handler.base_orm import VBOrmBase
from op_schedular.schema import VendorInvoiceSchema
from op_schedular.conf import get_logger, li_db
from typing import List


logger = get_logger()


class VbVendorInvoice(VBOrmBase):
    __table_name__ = 'vendor_invoice'
    __schema__ = VendorInvoiceSchema()

    def get_order_id_of_sonumber(
        self,
        vendor_id: int,
        so_number: str
    ) -> List:
        """
        Get record from order_bill table for given vendor_id and so_number
        """
        try:
            with li_db.transaction(auto_commit=True) as query_set:
                result_set = query_set.execute_query(
                    QUERY_SELECT_ORDER_ID_FOR_GIVEN_SO_NUMBER,
                    {
                        "vendor_id": vendor_id,
                        "so_number":  so_number
                    }
                )
                logger.debug(f'Executed Query {query_set.query}')
                result_set = result_set.to_list()
                return result_set
        except Exception as ex:
            logger.error("error while executing the given query that expect results", exc_info=True)
            raise ex

    def get_order_id_of_ponumber(
        self,
        vendor_id: int,
        po_number: str
    ) -> List:
        """
        Get record from order_bill table for given vendor_id and po_number
        """
        try:
            with li_db.transaction(auto_commit=True) as query_set:
                result_set = query_set.execute_query(
                    QUERY_SELECT_ORDER_ID_FOR_GIVEN_PO_NUMBER,
                    {
                        "vendor_id": vendor_id,
                        "po_number": po_number
                    }
                )
                logger.debug(f'Executed Query {query_set.query}')
                result_set = result_set.to_list()
                return result_set
        except Exception as ex:
            logger.error("error while executing the given query that expect results", exc_info=True)
            raise ex

    def get_invoice_numbers_of_purchase_order(
        self,
        orderid: int
    ) -> List:
        """
        Get list of order_bills for given purchase_order_id
        """
        try:
            with li_db.transaction(auto_commit=True) as query_set:
                result_set = query_set.execute_query(
                    QUERY_FETCH_INVOICE_NUMBER_FOR_ORDER,
                    {
                        "order_id": orderid
                    }
                )
                logger.debug(f'Executed Query {query_set.query}')
                result_set = result_set.to_list()
                return result_set
        except Exception as ex:
            logger.error("error while getting the invoice list", exc_info=True)
            raise ex

    def bulk_update_or_create(
        self,
        data: List
    ) -> List:

        if len(data) < 1:
            logger.info("Not enough bills to load")
            return

        columns = ", ".join(data[0].keys())
        placeholders = ", ".join(['%s'] * len(data))
        sql = QUERY_UPSERT_INTO_ORDER_BILL % (
            self.__table_name__,
            columns,
            placeholders,
        )

        value_list = [tuple(obj.values()) for obj in data]

        logger.info("Loading purchase order details from netsuite into DB")
        try:
            with li_db.transaction(auto_commit=True) as query_set:
                order_bill_ids = query_set.execute_query(sql, tuple(value_list))
                order_bill_ids = order_bill_ids.to_list()
                logger.debug(f'Executed Query {sql}')
        except Exception as e:
            logger.error(f'Error while executing {sql}', exc_info=True)
            raise e

        return order_bill_ids
