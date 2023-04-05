create TABLE IF NOT EXISTS stg.ordersystem_restaurants (
	id serial NOT NULL,
	object_id varchar UNIQUE NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL
	);