from logging import Logger
from typing import List

import json

from lib.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

class OrderObj(BaseModel):
    id: int
    object_value: str
    order_ts: datetime

class OrderSTGRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_orders(self, ts_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_value, order_ts
                    FROM stg.cs_deliveries
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": ts_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDDSRepository:
    def __init__(self, log: Logger) -> None:
        self.log = log

    def insert_entry(self, conn: Connection, order: OrderObj) -> None:
        
        orderDict = json.loads(order.object_value)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.cs_orders(order_id, order_ts, sum)
                    VALUES (%(order_id)s, %(order_ts)s, %(sum)s)
                """,
                {
                    "order_id": orderDict["order_id"],
                    "order_ts": order.order_ts,
                    "sum": orderDict["sum"]
                }
            )


class OrderLoader:
    WF_KEY = "cs_orders_stg_to_dds"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.origin  = OrderSTGRepository(pg_conn)
        self.stg     = OrderDDSRepository(log)
        self.log     = log
        self.settings_repository = StgEtlSettingsRepository("dds")

    def load_orders(self):

        with self.pg_conn.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.get_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} new orders in stg.cs_deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for c in load_queue:
                self.stg.insert_entry(conn, c)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([r.id for r in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

