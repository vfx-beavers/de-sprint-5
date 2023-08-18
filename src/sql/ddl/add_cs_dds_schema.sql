
DROP TABLE IF EXISTS dds.cs_timestamps CASCADE;
CREATE TABLE dds.cs_timestamps
(
	id serial PRIMARY KEY,
	ts timestamp unique not null,
	year smallint not null CONSTRAINT dm_timestamps_year_check 
		CHECK ((year >= 2022) and (year < 2500)),
	month smallint not null CONSTRAINT dm_timestamps_month_check 
		CHECK ((month >= 1) and (month <= 12)),
	day smallint not null CONSTRAINT dm_timestamps_day_check 
		CHECK ((day >= 1) and (day <= 31)),
	time time not null,
	date date not null
);

DROP TABLE IF EXISTS dds.cs_restaurants CASCADE;
CREATE TABLE dds.cs_restaurants(
	id serial PRIMARY KEY,
	restaurant_id varchar(100) UNIQUE not null,
	restaurant_name varchar(100) not null
);

DROP TABLE IF EXISTS dds.cs_couriers CASCADE;
CREATE TABLE dds.cs_couriers(
	id serial PRIMARY KEY,
	courier_id varchar(100) UNIQUE not null,
	courier_name varchar(100) not null
);

DROP TABLE IF EXISTS dds.cs_orders CASCADE;
CREATE TABLE dds.cs_orders(
	id serial PRIMARY KEY,
	order_id varchar(100) unique not null,
	order_ts timestamp not null,
--	restaurant_id int default (1),
	sum numeric(14,2) not null default 0 check (sum >= 0),
    CONSTRAINT cs_orders_timestamps_id_fkey FOREIGN KEY (order_ts) 
        REFERENCES dds.cs_timestamps(ts)
--    CONSTRAINT cs_orders_restaurants_id_fkey FOREIGN KEY (restaurant_id)
--        REFERENCES dds.cs_restaurants (id)
);


DROP TABLE IF EXISTS dds.cs_deliveries CASCADE;
CREATE TABLE dds.cs_deliveries(
	id serial PRIMARY KEY,
	delivery_id varchar(100) unique not null,
	courier_id int,
	order_id int,
	address varchar (255) not null,
	delivery_ts timestamp,
	rate int4 not null check (rate >= 1 and rate <= 5),
	tip_sum numeric(14,2) not null default 0 check (tip_sum >= 0),
    CONSTRAINT cs_deliveries_courier_id_fkey FOREIGN KEY (courier_id) 
        REFERENCES dds.cs_couriers(id),
    CONSTRAINT cs_deliveries_orders_id_fkey FOREIGN KEY (order_id)
        REFERENCES dds.cs_orders (id)
);

