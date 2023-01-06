from op_netsuite_fetcher.sql_queries import QUERY_UPSERT_PURCHASE_ORDER
from op_netsuite_fetcher.common.orm_handler.base_orm import VBOrmBase
from op_netsuite_fetcher.schema import PurchaseOrderSchema
from op_netsuite_fetcher.conf import get_logger, li_db


logger = get_logger()


class VbPurchaseOrder(VBOrmBase):
    __table_name__ = 'purchase_order'
    __schema__ = PurchaseOrderSchema()

    def bulk_update_or_create(
        self,
        data,
        conflict_fields: str,
        on_conflict_update_fields: str
    ):

        columns = ", ".join(data[0].keys())
        placeholders = ", ".join(['%s'] * len(data))
        sql = QUERY_UPSERT_PURCHASE_ORDER % (
            self.__table_name__,
            columns,
            placeholders,
            conflict_fields,
            on_conflict_update_fields
        )

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
