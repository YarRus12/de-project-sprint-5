CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
id serial UNIQUE NOT NULL,
restaurant_id varchar NOT NULL UNIQUE,
restaurant_name  varchar NOT NULL,
active_from  timestamp NOT NULL,
active_to timestamp NOT NULL
);