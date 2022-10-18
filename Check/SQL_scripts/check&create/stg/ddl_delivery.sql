CREATE TABLE IF NOT EXISTS stg.delivery (
id serial PRIMARY KEY,
order_id varchar NOT NULL UNIQUE,
order_ts timestamp NOT NULL,
delivery_id varchar NOT NULL,
courier_id varchar NOT NULL,
address varchar NOT NULL,
delivery_ts timestamp NOT NULL,
rate integer NOT NULL,
sum numeric(14,6),
tip_sum numeric(14,6));
