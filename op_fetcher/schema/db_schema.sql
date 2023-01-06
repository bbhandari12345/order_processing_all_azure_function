BEGIN;

DROP TABLE IF EXISTS ns_customer_invoice;
DROP TABLE IF EXISTS ns_vendor_bill;
DROP TABLE IF EXISTS ns_item_fulfillment;
DROP TABLE IF EXISTS vendor_invoice_items;
DROP TABLE IF EXISTS vendor_invoice;
DROP TABLE IF EXISTS purchase_order;
DROP TABLE IF EXISTS sales_order;


CREATE TABLE IF NOT EXISTS sales_order (
	id SERIAL,
	soint_id INT NOT NULL,
	invoiced BOOLEAN NOT NULL DEFAULT false,
	sales_order_status TEXT NULL,
	created_at TIMESTAMP without time zone NOT NULL default(current_timestamp),
	modified_at timestamp without time zone,
	PRIMARY KEY(id),
	CONSTRAINT uk_sales_order UNIQUE(soint_id)
);

CREATE FUNCTION sync_modified_at() RETURNS trigger AS $sync_modified_at$
BEGIN
	NEW.modified_at = current_timestamp;
	RETURN NEW;
END;
$sync_modified_at$ LANGUAGE plpgsql;

CREATE TRIGGER sync_modified_at_of_sales_order_table BEFORE UPDATE ON sales_order
FOR EACH ROW EXECUTE FUNCTION sync_modified_at();


CREATE TABLE IF NOT EXISTS purchase_order (
	id SERIAL,
	sales_order_id INT NOT NULL,
	point_id INT NOT NULL,
	tran_date DATE NULL,
	vendor_id INT NOT NULL,
	vendor_po_number TEXT NULL,
	purchase_order_status TEXT NULL,
	vendor_so_number TEXT NULL,
	vendor_name TEXT NULL,
	need_serial_number BOOLEAN NULL,
	created_at TIMESTAMP without time zone NOT NULL default(current_timestamp),
	modified_at timestamp without time zone,
	PRIMARY KEY(id),
	CONSTRAINT uk_purchase_order UNIQUE(point_id),
	CONSTRAINT fk_sales_order_id FOREIGN KEY(sales_order_id) REFERENCES sales_order(id)
);

CREATE TRIGGER sync_modified_at_of_purchase_order_table BEFORE UPDATE ON purchase_order
FOR EACH ROW EXECUTE FUNCTION sync_modified_at();


CREATE TABLE IF NOT EXISTS vendor_invoice (
	id SERIAL,
	purchase_order_id INT NOT NULL,
	memo TEXT NULL,
	tracking_numbers TEXT NULL,
	ship_date DATE NULL,
	deliveries JSON NULL,
	invoice_status TEXT NULL,
	invoice_status_raw TEXT NULL,
	tran_id TEXT NULL,
	tran_date DATE NULL,
	ship_cost NUMERIC NULL,
	total NUMERIC NULL,
	raw_total TEXT NULL,
	extra_item_price NUMERIC NULL,
	invoice_number TEXT NULL,
	tax_amount NUMERIC NULL,
	created_at TIMESTAMP without time zone NOT NULL default(current_timestamp),
	modified_at TIMESTAMP without time zone,
	PRIMARY KEY(id),
	CONSTRAINT fk_purchase_order_id FOREIGN KEY(purchase_order_id) REFERENCES purchase_order(id)
);

CREATE UNIQUE INDEX index_purchase_order_invoice_number ON vendor_invoice (
   purchase_order_id,
   COALESCE(invoice_number, '-1')
);

CREATE TRIGGER sync_modified_at_of_order_bill_table BEFORE UPDATE ON vendor_invoice
FOR EACH ROW EXECUTE FUNCTION sync_modified_at();


CREATE TABLE IF NOT EXISTS vendor_invoice_items (
	id SERIAL,
	vendor_invoice_id INT NOT NULL,
	itemno TEXT NULL,
	mfg_itemno TEXT NULL,
	item_details JSON NULL,
	amount NUMERIC NULL,
	rate NUMERIC NULL,
	quantity_ordered INT NULL,
	quantity INT NULL,
	raw_quantity INT NULL,
	quantity_backordered INT NULL,
	created_at TIMESTAMP without time zone NOT NULL default(current_timestamp),
	PRIMARY KEY(id),
	CONSTRAINT fk_invoice_id FOREIGN KEY(vendor_invoice_id) REFERENCES vendor_invoice(id)
);


CREATE TABLE IF NOT EXISTS ns_item_fulfillment (
	id SERIAL,
	vendor_invoice_id INT NOT NULL,
	fulfillment_id TEXT NULL,
	success BOOLEAN NULL,
	message TEXT NULL,
	created_at TIMESTAMP without time zone NOT NULL default(current_timestamp),
	PRIMARY KEY(id),
	CONSTRAINT fk_invoice_id_if FOREIGN KEY(vendor_invoice_id) REFERENCES vendor_invoice(id)
);


CREATE TABLE IF NOT EXISTS ns_vendor_bill (
	id SERIAL,
	vendor_invoice_id INT NOT NULL,
	vendor_bill_id TEXT NULL,
	success BOOLEAN NULL,
	message TEXT NULL,
	created_at TIMESTAMP without time zone NOT NULL default(current_timestamp),
	PRIMARY KEY(id),
	CONSTRAINT fk_invoice_id_vb FOREIGN KEY(vendor_invoice_id) REFERENCES vendor_invoice(id)
);


CREATE TABLE IF NOT EXISTS ns_customer_invoice (
	id SERIAL,
	sales_order_id INT NOT NULL,
	customer_invoice_id TEXT NULL,
	success BOOLEAN NULL,
	message TEXT NULL,
	created_at TIMESTAMP without time zone NOT NULL default(current_timestamp),
	PRIMARY KEY(id),
	CONSTRAINT fk_sales_order_id_vi FOREIGN KEY(sales_order_id) REFERENCES sales_order(id)
);


COMMIT;

-- ROLLBACK;
