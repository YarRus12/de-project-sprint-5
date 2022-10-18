CREATE TABLE IF NOT EXISTS dds.dm_products (
id serial UNIQUE NOT NULL,
restaurant_id integer NOT NULL,
product_id varchar NOT NULL,
product_name varchar NOT NULL,
product_price numeric(14,2) NOT NULL DEFAULT 0 CHECK (product_price>=0 and product_price<=999000000.99),
active_from  timestamp NOT NULL,
active_to timestamp NOT NULL
);
ALTER TABLE dds.dm_products ADD CONSTRAINT dm_products_restaurant_id_fkey  FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id) ON UPDATE CASCADE;