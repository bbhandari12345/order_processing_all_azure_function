from op_dispatcher.sql_queries import QUERY_UPSERT_INTO_ORDER_BILL_ITEMS
from op_dispatcher.common.orm_handler.base_orm import VBOrmBase
from op_dispatcher.schema import VendorInvoiceItemsSchema
from op_dispatcher.conf import get_logger, li_db
from typing import List


logger = get_logger()


class VbVendorInvoiceItems(VBOrmBase):
    __table_name__ = 'vendor_invoice_items'
    __schema__ = VendorInvoiceItemsSchema()

    def bulk_update_or_create(
        self,
        data: List,
        bill_ids: set
    ) -> None:
        columns = ", ".join(data[0].keys())
        placeholders = ", ".join(['%s'] * len(data))
        sql = QUERY_UPSERT_INTO_ORDER_BILL_ITEMS % {
                "table_name": self.__table_name__,
                "vendor_invoice_id": tuple(bill_ids),
                "column_names": columns,
                "values": placeholders
            }
        

        value_list = [tuple(obj.values()) for obj in data]

        logger.info("Loading purchase order details from netsuite into DB")
        try:
            with li_db.transaction(auto_commit=True) as query_set:
                query_set.execute_query(sql, tuple(value_list))
                logger.debug(f'Executed Query {sql}')
        except Exception as e:
            logger.error(f'Error while executing {sql}', exc_info=True)
            raise e

        return
