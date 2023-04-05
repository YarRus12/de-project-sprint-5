--DROP TABLE IF EXISTS cdm.dm_courier_ledger CASCADE;
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger
(id serial PRIMARY KEY,
courier_id integer NOT NULL,
courier_name varchar(255) NOT NULL,
settlement_year integer NOT NULL CHECK (settlement_year>='2022' AND settlement_year < '2500'),
settlement_month integer NOT NULL CHECK(((settlement_month >= '1') AND (settlement_month <= '12'))),
orders_count integer NOT NULL CHECK (orders_count >= 0),
orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= (0)::numeric),
rate_avg numeric(14,2) NOT NULL DEFAULT 1,
order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= (0)::numeric),
courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= (0)::numeric),
courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= (0)::numeric),
courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= (0)::numeric));