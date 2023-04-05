CREATE TABLE IF NOT EXISTS dds.dm_orders (
id serial PRIMARY KEY,
user_id integer NOT NULL,
restaurant_id integer NOT NULL,
timestamp_id integer NOT NULL,
order_key varchar UNIQUE NOT NULL,
order_status varchar NOT NULL);


ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_dm_restaurants_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id) ON UPDATE CASCADE;
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_dm_timestamps_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id) ON UPDATE CASCADE;
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_dm_users_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id) ON UPDATE CASCADE;
