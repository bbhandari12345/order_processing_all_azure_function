from marshmallow import fields, Schema, pre_load
import pandas as pd
import json


class SalesOrderSchema(Schema):
    id = fields.Int(dump_only=True)
    soint_id = fields.Str(allow_none=False)
    invoiced = fields.Boolean(allow_none=False, load_default=False)
    sales_order_status = fields.Str(allow_none=True)
    created_at = fields.AwareDateTime(dump_only=True)
    modified_at = fields.DateTime(allow_none=False)


class PurchaseOrderSchema(Schema):
    id = fields.Int(dump_only=True)
    sales_order_id = fields.Int(allow_none=False)
    point_id = fields.Int(allow_none=False)
    tran_date = fields.Date(allow_none=True)
    vendor_id = fields.Int(allow_none=True)
    vendor_po_number = fields.Str(allow_none=True)
    purchase_order_status = fields.Str(allow_none=True)
    vendor_so_number = fields.Str(allow_none=True)
    vendor_name = fields.Str(allow_none=True)
    need_serial_number = fields.Bool(allow_none=True)
    created_at = fields.AwareDateTime(dump_only=True)
    modified_at = fields.DateTime(allow_none=False)

    @pre_load
    def get_possible_values(self, item, many, **kwargs):
        """There is no default field type for json/jsonb
            Using the method field to return the required data
        """
        if "tran_date" in item and isinstance(item['tran_date'], str):
            item['tran_date'] = pd.to_datetime(item['tran_date']).date().isoformat()
        return item


class VendorInvoiceSchema(Schema):
    id = fields.Int(dump_only=True)
    purchase_order_id = fields.Int(allow_none=False)
    memo = fields.Str(allow_none=True, description='String')
    ship_date = fields.Date(allow_none=True)
    deliveries = fields.Raw(allow_none=True, description='Raw')
    invoice_status = fields.Str(allow_none=True, description='String')
    invoice_status_raw = fields.Str(allow_none=True, description='String')
    tran_id = fields.Str(allow_none=True, description='String')
    tran_date = fields.Date(allow_none=True)
    ship_cost = fields.Float(allow_none=True, description='Int')
    total = fields.Float(allow_none=True, description='Int')
    raw_total = fields.Str(allow_none=True, description='String')
    extra_item_price = fields.Float(allow_none=True, description='Int')
    invoice_number = fields.Str(allow_none=True, description='String')
    tax_amount = fields.Float(allow_none=True, description='Int')
    created_at = fields.AwareDateTime(dump_only=True)
    modified_at = fields.DateTime(allow_none=False)

    @pre_load
    def get_possible_values(self, item, many, **kwargs):
        """There is no default field type for json/jsonb
            Using the method field to return the required data
        """

        for fld in self._declared_fields:
            if fld in (
                'memo',
                'ship_date',
                'invoice_status',
                'invoice_status_raw',
                'tran_id',
                'tran_date',
                'ship_cost',
                'total',
                'raw_total',
                'extra_item_price',
                'invoice_number',
                'tax_amount'
            ):
                if (
                    self._declared_fields.get(fld).metadata.get('description') == 'String'
                    and not item.get(fld)
                ):
                    item[fld] = None
                elif (
                    self._declared_fields.get(fld).metadata.get('description') == 'Int'
                    and item.get(fld) is None
                ):
                    item[fld] = None
                else:
                    pass

        if "ship_date" in item and isinstance(item['ship_date'], str):
            item['ship_date'] = pd.to_datetime(item['ship_date']).date().isoformat()
        if "tran_date" in item and isinstance(item['tran_date'], str):
            item['tran_date'] = pd.to_datetime(item['tran_date']).date().isoformat()

        if 'deliveries' in item and not item['deliveries']:
            item['deliveries'] = None
        elif 'deliveries' in item and item['deliveries']:
            item['deliveries'] = json.dumps(item['deliveries'])
        else:
            pass

        return item


class VendorInvoiceItemsSchema(Schema):
    id = fields.Int(dump_only=True)
    vendor_invoice_id = fields.Int()
    itemno = fields.Str(allow_none=True, description='String')
    mfg_itemno = fields.Str(allow_none=True, description='String')
    item_details = fields.Raw(allow_none=True, description='Raw')
    amount = fields.Float(allow_none=True, description='Int')
    rate = fields.Float(allow_none=True, description='Int')
    quantity_ordered = fields.Int(allow_none=True, description='Int')
    quantity = fields.Int(allow_none=True, description='Int')
    raw_quantity = fields.Int(allow_none=True, description='Int')
    quantity_backordered = fields.Int(allow_none=True, description='Int')
    created_at = fields.AwareDateTime(dump_only=True)

    @pre_load
    def get_possible_values(self, item, many, **kwargs):
        """There is no default field type for json/jsonb
            Using the method field to return the required data
        """
        for fld in self._declared_fields:
            if fld in (
                'mfg_itemno',
                'itemno',
                'amount',
                'rate',
                'quantity_ordered',
                'quantity',
                'quantity_backordered',
                "raw_quantity"
            ):
                if (
                    self._declared_fields.get(fld).metadata.get('description') == 'String'
                    and not item.get(fld)
                ):
                    item[fld] = None
                elif (
                    self._declared_fields.get(fld).metadata.get('description') == 'Int'
                    and item.get(fld) is None
                ):
                    item[fld] = None
                else:
                    pass

        if 'item_details' in item and not item['item_details']:
            item['item_details'] = None
        elif 'item_details' in item and item['item_details']:
            item['item_details'] = json.dumps(item['item_details'])
        else:
            pass

        return item 


class NsItemFulfillmentSchema(Schema):
    id = fields.Int(dump_only=True)
    vendor_invoice_id = fields.Int()
    fulfillment_id = fields.Str(allow_none=True)
    success = fields.Bool(allow_none=True) 
    message = fields.Str(allow_none=True)
    created_at = fields.AwareDateTime(dump_only=True)


class NsVendorBillSchema(Schema):
    id = fields.Int(dump_only=True)
    vendor_invoice_id = fields.Int()
    vendor_bill_id = fields.Str(allow_none=True)
    success = fields.Bool(allow_none=True) 
    message = fields.Str(allow_none=True) 
    created_at = fields.AwareDateTime(dump_only=True)


class NsCustomerInvoiceSchema(Schema):
    id = fields.Int(dump_only=True)
    sales_order_id = fields.Int()
    customer_invoice_id = fields.Str(allow_none=True)
    success = fields.Bool(allow_none=True) 
    message = fields.Str(allow_none=True) 
    created_at = fields.AwareDateTime(dump_only=True)
