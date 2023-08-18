DROP TABLE IF EXISTS cdm.dm_courier_ledger CASCADE;

CREATE TABLE cdm.dm_courier_ledger(
	id serial PRIMARY KEY,
	courier_id varchar(100) not null,
	courier_name varchar(100) not null,
	settlement_year integer not null check(settlement_year >= 2022 and  settlement_year < 2500),
	settlement_month int4 not null check(settlement_month >= 1 and settlement_month <= 12),
	orders_count integer not null check (orders_count >= 0) default (0),
	order_total_sum numeric(14,2) not null check (order_total_sum >= 0) default (0),
	rate_avg numeric(14,2) not null check (rate_avg >=0) default (0),
	order_processing_fee numeric(14,2) not null check (order_processing_fee >= 0) default (0),
	courier_order_sum numeric(14,2) not null check (courier_order_sum >= 0) default (0),
	courier_tips_sum numeric(14,2) not null check (courier_tips_sum >= 0) default (0),
	courier_reward_sum numeric(14,2) not null check (courier_reward_sum >= 0) default (0)
);

ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_unique
UNIQUE (courier_id, settlement_year, settlement_month);