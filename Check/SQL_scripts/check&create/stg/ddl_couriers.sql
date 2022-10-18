CREATE TABLE IF NOT EXISTS stg.couriers (
id serial UNIQUE NOT NULL,
courier_id varchar NOT NULL UNIQUE,
courier_name  varchar NOT NULL);