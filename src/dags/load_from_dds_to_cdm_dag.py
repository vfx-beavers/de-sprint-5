import logging
from airflow.decorators import dag, task
import pendulum
from cdm.courier_ledger_loader import CourierLedgerLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="Europe/Moscow"),
    catchup=False,
    tags=['s5_proj', 'api', 'cdm'],
    is_paused_upon_creation=False
)
def load_from_dds_to_cdm():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_courierledger_task():
        log.info(">>> add DDS to CDM")
        loader = CourierLedgerLoader(pg_conn = dwh_pg_connect, log = log)
        loader.load_datamart()

    courierledger  = load_courierledger_task()

    courierledger

load_dds_to_cdm_dag = load_from_dds_to_cdm()
