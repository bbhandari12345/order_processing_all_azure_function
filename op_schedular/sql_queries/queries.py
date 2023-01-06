QUERY_FETCH_VENDORS_INFORMATION = ("""
    SELECT jsonb_build_object(
        'vendor_id', vconfig.vendor_id,
        'config_file_path', %(config_file_path)s||vconfig.vendor_id::text||'.json',
        'template_values', jsonb_object_agg(key_name, value),
        'connection_type', ven.connection_type
    )
    FROM vendor_configs vconfig
    INNER JOIN purchase_order vo
    ON vconfig.vendor_id = vo.vendor_id
    INNER JOIN vendors ven
    ON ven.vendor_id = vconfig.vendor_id
    group by vconfig.vendor_id, ven.connection_type;
""")

QUERY_UPSERT_SALES_ORDER = ("""
    WITH insert_salesorder AS (
        INSERT INTO %s (%s)
        VALUES %s
        ON CONFLICT (%s) DO UPDATE SET %s
        returning id, soint_id
    )
    SELECT jsonb_agg(
        json_build_object(
            soint_id, id
        )
    )
    FROM insert_salesorder;
""")

QUERY_UPSERT_PURCHASE_ORDER = ("""
    INSERT INTO %s (%s)
    VALUES %s
    ON CONFLICT (%s)
    DO UPDATE SET %s;
""")

QUERY_UPDATE_SALESORDER_NUMBER_BY_CHECKING_SUBSTR = ("""
    UPDATE purchase_order
    SET vendor_so_number=left(vendor_so_number, length(vendor_so_number)-2)
    WHERE id in (
        SELECT id FROM purchase_order
        WHERE vendor_id = %d AND substr(vendor_so_number, length(vendor_so_number)-1) = '-S'
    );
""")

QUERY_SELECT_SALESORDER = (""" 
    SELECT o.vendor_so_number AS so_number, o.need_serial_number AS need_serial_no
    FROM purchase_order o 
    LEFT JOIN vendor_invoice f 
    ON o.id = f.purchase_order_id
    WHERE o.vendor_id=%s AND o.purchase_order_status in %s;
""")

QUERY_SELECT_ORDER_ID = ("""
    SELECT id FROM purchase_order 
    WHERE vendor_id=%s AND vendor_so_number='%s'
""")

QUERY_FETCH_INVOICE_NUMBERS = ("""
    SELECT invoice_number FROM vendor_invoice
    WHERE purchase_order_id = %s AND invoice_number IS NOT NULL;
""")

QUERY_SELECT_PURCHASE_ORDER_AND_SALESORDER = (""" 
    SELECT o.vendor_po_number AS po_number, o.vendor_so_number AS so_number, o.need_serial_number AS need_serial_no
    FROM purchase_order o 
    LEFT JOIN vendor_invoice f 
    ON o.id = f.purchase_order_id 
    WHERE o.vendor_id=%s AND o.purchase_order_status in %s;
""")

QUERY_SELECT_ORDER_ID_FOR_GIVEN_SO_NUMBER = ("""
    SELECT id FROM purchase_order
    WHERE vendor_id=%(vendor_id)s AND vendor_so_number=%(so_number)s;
""")

QUERY_SELECT_ORDER_ID_FOR_GIVEN_PO_NUMBER = ("""
    SELECT id FROM purchase_order
    WHERE vendor_id=%(vendor_id)s AND vendor_po_number=%(po_number)s;
""")

QUERY_FETCH_INVOICE_NUMBER_FOR_ORDER = ("""
    SELECT invoice_number FROM vendor_invoice
    WHERE purchase_order_id = %(order_id)s
    AND invoice_number IS NOT NULL;
""")

QUERY_UPSERT_INTO_ORDER_BILL = ("""
    WITH insert_bill AS (              
        INSERT INTO %s (%s)
        VALUES %s
        ON CONFLICT (purchase_order_id, COALESCE(invoice_number, '-1'))
        DO UPDATE SET invoice_status=EXCLUDED.invoice_status, deliveries=EXCLUDED.deliveries
        RETURNING purchase_order_id, id AS order_bill_id
    )
    SELECT jsonb_agg(
        json_build_object(
            purchase_order_id, order_bill_id
        )
    )
    FROM insert_bill;
""")

QUERY_UPSERT_INTO_ORDER_BILL_ITEMS = ("""
    DELETE FROM %(table_name)s WHERE vendor_invoice_id IN %(vendor_invoice_id)s;
    INSERT INTO %(table_name)s (%(column_names)s) VALUES %(values)s;
""")

QUERY_FETCH_VENDOR_BILL_OBJECT = ("""
WITH items AS (
    SELECT invoice_id, jsonb_agg(
        CASE WHEN mfg_itemno IS NOT NULL
        THEN
            json_build_object(
                'itemno', CASE WHEN itemno IS NOT NULL AND itemno <> '' THEN itemno ELSE mfg_itemno END,
                'amount', amount,
                'rate', rate,
                'quantity', quantity,
                'mfg_itemno', mfg_itemno
            )
        ELSE
            json_build_object(
                'itemno', itemno,
                'amount', amount,
                'rate', rate,
                'quantity', quantity
            )
        END
    )
    FROM vendor_invoice_items
    WHERE quantity > 0
    GROUP BY invoice_id
)
SELECT vo.point_id, vo.soint_id, vo.vendor_id, items.jsonb_agg AS items,
vin.tracking_numbers,
CASE
    WHEN vin.shipdate IS NOT NULL AND vin.shipdate <> ''
    THEN 'T'
    ELSE 'F'
END AS shipcomplete,
vin.shipdate, vin.tranid,
CASE
    WHEN vin.trandate_from_vendor IS NOT NULL THEN vin.trandate_from_vendor
    WHEN vin.shipdate IS NOT NULL THEN vin.shipdate
    ELSE vo.trandate
END AS trandate,
CASE
    WHEN vin.shipcost IS NOT NULL
    THEN vin.shipcost::float
    ELSE 0.00
END AS shipcost,
vo.vendorname, vin.total::float, vin.id,
38577 as extraitemintid, 1 as extraitemqty,
CASE
    WHEN vin.extraitemprice IS NOT NULL THEN vin.extraitemprice::float
    ELSE 0.00
END AS extraitemprice
FROM public.vb_order vo
INNER JOIN public.vb_invoice vin
ON vo.id = vin.orderid
INNER JOIN public.vb_items vit
ON vin.id = vit.invoice_id
RIGHT JOIN items
ON items.invoice_id=vin.id
WHERE vo.status_netsuite IN %(order_status)s
AND vin.success IS NULL AND vin.netsuite_message IS NULL AND vin.vendor_bill_created_id IS NULL
GROUP BY vo.pointid, vo.sointid, vo.status_netsuite, vo.vendorid, vin.trackingnumbers,
vin.shipdate,vin.tranid, vin.trandate_from_vendor, vo.vendorname,
vin.total, vin.shipcost, vin.id, vo.trandate, items.jsonb_agg;
""")

QUERY_UPDATE_CREATED_VENDOR_BILL_ID = (
"""
    UPDATE %s
    SET %s
    FROM
    (
        SELECT unnest(%s) as success,
        unnest(%s) as vendor_bill_created_id,
        unnest(%s) as netsuite_message,
        unnest(%s) as id
    ) AS tmp_table
    WHERE %s;
""")

QUERY_FETCH_NOT_FULLY_BILLED_PURCHASE_ORDER = ("""
    SELECT pointid FROM public.vb_order
    WHERE status_netsuite <> 'fullyBilled';
""")

QUERY_UPDATE_PURCHASE_ORDER_STATUS = (
"""
    UPDATE %s
    SET %s
    FROM
    (
        SELECT unnest(%s) as point_id,
        unnest(%s) as status
    ) AS tmp_table
    WHERE %s;
""")
