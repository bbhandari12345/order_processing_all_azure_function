from op_schedular.common.helpers import data_to_be_inserted_into_table
from op_schedular.orm import VbVendorInvoice, VbVendorInvoiceItems
from op_schedular.base_class import BaseDispatcher
from collections import ChainMap
from typing import Any


class OrderLoader(BaseDispatcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.items_list = []
        self._sonumber = None
        self.invoice_list = []

    def read_transformed(self) -> Any:
        self.logger.debug("Reading fetcher data")
        self.read_data()
        return self

    def loader(self) -> None:
        for obj in self.data:
            if obj.get('so_number'):
                self.logger.info("Fetching object from fulfillment table which has invoice on the basis of vendor_id and sonumber")
                # Get the order id from order table with given vendor_id, sonum
                result = VbVendorInvoice().get_order_id_of_sonumber(
                    vendor_id=self.kwargs.get("vendor_id"),
                    so_number=obj.get('so_number')
                )
            elif obj.get('po_number'):
                self.logger.info("Fetching object from fulfillment table which has invoice on the basis of vendor_id and ponumber")
                result = VbVendorInvoice().get_order_id_of_ponumber(
                    vendor_id=self.kwargs.get("vendor_id"),
                    po_number=obj.get('po_number')
                )
            else:
                self.logger.error("Couldnot fetch info from fulfillment since po_number or so_number are not provided")
                raise Exception("Couldnot fetch info from fulfillment since po_number or so_number are not provided")

            obj.setdefault('purchase_order_id', result[0].get('id'))

            # Get the invoice list from fulfillment table
            invoice_list = set([item['invoice_number'] for item in VbVendorInvoice().get_invoice_numbers_of_purchase_order(obj.get('purchase_order_id'))])
            self.logger.info("Discarding those orders that invoice in it")
            self.invoice_list.extend(invoice_list)

        self.data = data_to_be_inserted_into_table(
            data=self.data,
            invoice_list=self.invoice_list
        )
        return self
    
    def db_dispatcher(self) -> Any:
        """
        Loads data after transformation into Database
        """

        if not self.data:
            self.logger.info("No bills to Load. Skipping loading part")
            return

        for obj in self.data:
            if not obj.get('items'):
                continue

            self.items_list.append({
                obj.get("purchase_order_id"): obj.get("items")
            })
            del obj["items"]

        bills = VbVendorInvoice(self.data)
        bills.load()
        order_bill_ids = bills.bulk_update_or_create(bills.loaded_data)

        if not order_bill_ids:
            self.logger.info("Bills are not loaded")
            return
        elif not self.items_list:
            self.logger.info("No bill items to Load")
            return

        items_data = []
        dict_order_bill_ids = dict(ChainMap(*order_bill_ids[0]['jsonb_agg']))
        for k, v in dict(ChainMap(*self.items_list)).items():
            for obj in v:
                obj.update({
                    "vendor_invoice_id": dict_order_bill_ids[str(k)]
                })
                items_data.append(obj)

        bill_items = VbVendorInvoiceItems(items_data)
        bill_items.load()
        bill_items.bulk_update_or_create(bill_items.loaded_data, set(dict_order_bill_ids.values()))

        return
