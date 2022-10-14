--DROP TABLE IF EXISTS cdm.dm_settlement_report CASCADE; -- оставлено прозапас

CREATE SCHEMA IF NOT EXISTS cdm;
CREATE TABLE cdm.dm_settlement_report
(id serial PRIMARY KEY,
restaurant_id integer NOT NULL,
restaurant_name varchar(255) NOT NULL,
settlement_date date NOT NULL CHECK (settlement_date>='2022-01-01' AND settlement_date < '2500-01-01'),
orders_count integer NOT NULL,
orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0,
orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0,
order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_count_check CHECK (orders_count >= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK (orders_total_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK (orders_bonus_payment_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK (orders_bonus_granted_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK (order_processing_fee >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK (restaurant_reward_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_unique UNIQUE (restaurant_id, settlement_date);
