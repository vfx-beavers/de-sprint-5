import logging
from stg.restaurant_loader import RestaurantLoader
from stg.delivery_loader import DeliveryLoader
import pendulum

from lib import ConnectionBuilder
from stg.courier_loader import CourierLoader
from airflow.decorators import dag, task

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="Europe/Moscow"),
    catchup=False,
    tags=['s5_proj', 'api', 'stg'],
    is_paused_upon_creation=False
)
def load_from_api_to_stg():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task()
    def load_couriers_task():
        log.info(">>> add Couriers from api")
        courier_loader = CourierLoader(dwh_pg_connect, log)
        courier_loader.load_couriers()

    @task()
    def load_restaurants_task():
        log.info(">>> add Restaurants from api")
        restaurant_loader = RestaurantLoader(dwh_pg_connect, log)
        restaurant_loader.load_restaurants()

    @task()
    def load_deliveries_task():
        log.info(">>> add Deliveries from api")
        delivery_loader = DeliveryLoader(dwh_pg_connect, log)
        delivery_loader.load_deliveries()

    couriers    = load_couriers_task()
    restaurants = load_restaurants_task()
    deliveries  = load_deliveries_task()

    couriers
    restaurants
    deliveries

load_from_cs_dag = load_from_api_to_stg()
