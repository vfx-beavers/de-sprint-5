DROP TABLE IF EXISTS stg.cs_restaurants CASCADE;
CREATE TABLE stg.cs_restaurants(
    id serial,
    object_id varchar not null,
    object_value text not null,
    update_ts timestamp not null,
    CONSTRAINT cs_restaurants_object_id_unique UNIQUE (object_id)
);

DROP TABLE IF EXISTS stg.cs_couriers CASCADE;
CREATE TABLE stg.cs_couriers(
    id serial,
    object_id varchar not null,
    object_value text not null,
    update_ts timestamp not null,
    CONSTRAINT cs_couriers_object_id_unique UNIQUE (object_id)
);

DROP TABLE IF EXISTS stg.cs_deliveries CASCADE;
CREATE TABLE stg.cs_deliveries(
    id serial,
    object_id varchar not null,
    object_value text not null,
    delivery_ts timestamp not null,
    order_ts timestamp not null,
    update_ts timestamp not null,
    CONSTRAINT cs_deliveries_object_id_unique UNIQUE (object_id)
);