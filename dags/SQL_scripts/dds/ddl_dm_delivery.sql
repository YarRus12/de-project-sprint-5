CREATE TABLE IF NOT EXISTS dds.dm_delivery (
id serial PRIMARY KEY,
order_id varchar NOT NULL,
delivery_id varchar NOT NULL,
courier_id integer NOT NULL,
address varchar NOT NULL,
delivery_ts_id integer NOT NULL,
rate float NOT NULL,
order_sum numeric(14,6),
tip_sum numeric(14,6),
--CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id) ON UPDATE CASCADE,
CONSTRAINT fk_courier FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id) ON UPDATE CASCADE);
