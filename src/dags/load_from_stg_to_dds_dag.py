import logging

import pendulum
from dds.restaurants_loader import RestaurantLoader
from dds.couriers_loader import CourierLoader
from dds.orders_loader import OrderLoader
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from dds.deliveries_loader import DeliveryLoader
from dds.timestamps_loader import TsLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="Europe/Moscow"),
    catchup=False,
    tags=['s5project', 'couriersystem', 'dds'],
    is_paused_upon_creation=False
)
def load_from_stg_to_dds():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_timestamps_task():
        log.info(">>> add Timestamps to DDS")
        loader = TsLoader(pg_conn = dwh_pg_connect, log = log)
        loader.load_timestamps()

    @task()
    def load_restaurants_task():
        log.info(">>> add Restaurants to DDS")
        restaurant_loader = RestaurantLoader(dwh_pg_connect, log)
        restaurant_loader.load_restaurants()

    @task()
    def load_couriers_task():
        log.info(">>> add Couriers to DDS")
        courier_loader = CourierLoader(dwh_pg_connect, log)
        courier_loader.load_couriers()

    @task()
    def load_orders_task():
        log.info(">>> add Orders to DDS")
        order_loader = OrderLoader(dwh_pg_connect, log)
        order_loader.load_orders()

    @task()
    def load_deliveries_task():
        log.info(">>> add Deliveries to DDS")
        delivery_loader = DeliveryLoader(dwh_pg_connect, log)
        delivery_loader.load_deliveries()

    timestamps  = load_timestamps_task()
    restaurants = load_restaurants_task()
    couriers    = load_couriers_task()
    orders      = load_orders_task()
    deliveries  = load_deliveries_task()

    [timestamps, restaurants, couriers] >> orders >> deliveries

load_stg_to_dds_dag = load_from_stg_to_dds()
