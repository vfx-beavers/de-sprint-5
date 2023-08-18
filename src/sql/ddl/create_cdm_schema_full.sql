DROP SCHEMA IF EXISTS cdm CASCADE;

CREATE SCHEMA cdm AUTHORIZATION jovyan;

-- DROP SEQUENCE cdm.dm_settlement_report_id_seq;

CREATE SEQUENCE cdm.dm_settlement_report_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- cdm.dm_settlement_report definition

-- Drop table

-- DROP TABLE cdm.dm_settlement_report;

CREATE TABLE cdm.dm_settlement_report (
	id serial4 NOT NULL,
	restaurant_id varchar(100) NOT NULL,
	restaurant_name varchar(100) NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 NOT NULL DEFAULT 0,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_settlement_report_pkey PRIMARY KEY (id),
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (((settlement_date >= '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date))),
	CONSTRAINT dm_settlement_report_settlement_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_settlement_orders_bonus_granted_sum_check CHECK ((orders_bonus_granted_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_settlement_orders_bonus_payment_sum_check CHECK ((orders_bonus_payment_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_settlement_orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_settlement_report_settlement_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_settlement_restaurant_id_settlement_date_u UNIQUE (restaurant_id, settlement_date),
	CONSTRAINT dm_settlement_report_settlement_restaurant_reward_sum_check CHECK ((restaurant_reward_sum >= (0)::numeric))
);


