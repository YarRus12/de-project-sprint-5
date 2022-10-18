CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
id serial PRIMARY KEY,
product_id integer NOT NULL,
order_id integer NOT NULL,
count integer NOT NULL DEFAULT 0 CHECK(count>=0),
price numeric(14,2) NOT NULL DEFAULT 0 CHECK(price>=0),
total_sum numeric(14,2) NOT NULL DEFAULT 0 CHECK(total_sum>=0),
bonus_payment numeric(14, 2) NOT NULL DEFAULT 0 CHECK(bonus_payment>=0),
bonus_grant numeric(14, 2) NOT NULL DEFAULT 0 CHECK(bonus_grant>=0));


ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_dm_products_fk FOREIGN KEY (product_id) REFERENCES dds.dm_products(id) ON UPDATE CASCADE;
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_dm_orders_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id) ON UPDATE CASCADE;
