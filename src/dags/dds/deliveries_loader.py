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

class DeliveryObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    delivery_ts: datetime

class IdObj(BaseModel):
    order_id: int
    courier_id: int

class DeliverySTGRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_deliveries(self, ts_threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, delivery_ts
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


class DeliveryDDSRepository:

    def __init__(self, log: Logger) -> None:
        self.log = log

    def insert_entry(self, conn: Connection, delivery: DeliveryObj) -> None:
        
        deliveryDict = json.loads(delivery.object_value)

        with conn.cursor(row_factory=class_row(IdObj)) as cur:

            cur.execute(
                """
                    select 
                        ord.id as order_id, cour.id as courier_id
                    from 
                        dds.cs_couriers cour, dds.cs_orders ord
                    where 
                        ord.order_id = %(order_key)s and cour.courier_id = %(courier_key)s
                """, 
                {
                    "order_key":   deliveryDict["order_id"],
                    "courier_key": deliveryDict["courier_id"]
                }
            )
            id = cur.fetchone()

            cur.execute(
                """
                    INSERT INTO dds.cs_deliveries(delivery_id, delivery_ts, order_id, courier_id, address, rate, tip_sum)
                    VALUES (%(delivery_id)s, %(delivery_ts)s, %(order_id)s, %(courier_id)s, %(address)s, %(rate)s, %(tip_sum)s)
                """,
                {
                    "delivery_id": delivery.object_id,
                    "delivery_ts": delivery.delivery_ts,
                    "order_id":    id.order_id,
                    "courier_id":  id.courier_id,
                    "address": deliveryDict["address"],
                    "rate":    deliveryDict["rate"],
                    "tip_sum": deliveryDict["tip_sum"]
                }
            )


class DeliveryLoader:
    WF_KEY = "cs_deliveries_stg_to_dds"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.origin  = DeliverySTGRepository(pg_conn)
        self.stg     = DeliveryDDSRepository(log)
        self.log     = log
        self.settings_repository = StgEtlSettingsRepository("dds")

    def load_deliveries(self):

        with self.pg_conn.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.get_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} new deliveries in stg.cs_deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for c in load_queue:
                self.stg.insert_entry(conn, c)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([r.id for r in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

