drop schema if exists stg CASCADE;
CREATE SCHEMA stg AUTHORIZATION jovyan;

-- DROP SEQUENCE stg.ordersystem_orders_id_seq;

CREATE SEQUENCE stg.ordersystem_orders_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE stg.ordersystem_restaurants_id_seq;

CREATE SEQUENCE stg.ordersystem_restaurants_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE stg.ordersystem_users_id_seq;

CREATE SEQUENCE stg.ordersystem_users_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE stg.srv_wf_settings_id_seq;

CREATE SEQUENCE stg.srv_wf_settings_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- stg.bonussystem_events definition

-- Drop table

-- DROP TABLE stg.bonussystem_events;

CREATE TABLE stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT bonussystem_events_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_bonussystem_events_event_ts ON stg.bonussystem_events USING btree (event_ts);


-- stg.bonussystem_ranks definition

-- Drop table

-- DROP TABLE stg.bonussystem_ranks;

CREATE TABLE stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL,
	min_payment_threshold numeric(19, 5) NOT NULL,
	CONSTRAINT bonussystem_ranks_pkey PRIMARY KEY (id)
);


-- stg.bonussystem_users definition

-- Drop table

-- DROP TABLE stg.bonussystem_users;

CREATE TABLE stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT bonussystem_users_pkey PRIMARY KEY (id)
);


-- stg.ordersystem_orders definition

-- Drop table

-- DROP TABLE stg.ordersystem_orders;

CREATE TABLE stg.ordersystem_orders (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_orders_pkey PRIMARY KEY (id)
);


-- stg.ordersystem_restaurants definition

-- Drop table

-- DROP TABLE stg.ordersystem_restaurants;

CREATE TABLE stg.ordersystem_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);


-- stg.ordersystem_users definition

-- Drop table

-- DROP TABLE stg.ordersystem_users;

CREATE TABLE stg.ordersystem_users (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
);


-- stg.srv_wf_settings definition

-- Drop table

-- DROP TABLE stg.srv_wf_settings;

CREATE TABLE stg.srv_wf_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);
