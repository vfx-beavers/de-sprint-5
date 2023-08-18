from logging import Logger
from typing import List

from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import json
from datetime import datetime
from wsgiref import headers
import requests
from lib.stg_settings_repository import EtlSetting, StgEtlSettingsRepository

from stg.courier_system_api_caller import CourierSystem

class TsObj(BaseModel):
    max_ts: datetime

class DeliveryRepository:

    def insert_entry(self, conn: Connection, obj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.cs_deliveries(object_id, object_value, delivery_ts, order_ts, update_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(delivery_ts)s, %(order_ts)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO NOTHING;
                """,
                {
                    "object_id": obj["delivery_id"],
                    "object_value": json.dumps(obj),
                    "delivery_ts": obj["delivery_ts"],
                    "order_ts": obj["order_ts"],
                    "update_ts": datetime.now()
                },
            )

    def get_max_delivery_ts(self, conn: Connection) -> TsObj:

        with conn.cursor(row_factory=class_row(TsObj)) as cur:
            cur.execute("SELECT max(delivery_ts) as max_ts FROM stg.cs_deliveries", {})
            res = cur.fetchone()
        return res

class DeliveryLoader:

    WF_KEY = "deliveries_from_couriersystem_to_stg"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierSystem(log)
        self.stg = DeliveryRepository()
        self.log = log
        self.settings_repository = StgEtlSettingsRepository("stg")

    def load_deliveries(self):

        i = 1
        limit  = 50
        offset = 0

        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            self.log.info(f"last_loaded_ts_str: {last_loaded_ts_str}")
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            while True:

                load_queue = self.origin.get_deliveries_from_api(limit, offset, last_loaded_ts.replace(microsecond=0))

                self.log.info(f"Reveived {i} batch of {len(load_queue)} deliveries to load.")
                if not load_queue:
                    self.log.info("Nothing received, stopping.")
                    break

                # Сохраняем объекты в базу dwh.
                for c in load_queue:
                    self.stg.insert_entry(conn, c)

                if not load_queue or len(load_queue) < limit:
                    break

                offset +=  limit
                i += 1

            ts = self.stg.get_max_delivery_ts(conn)
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = ts.max_ts
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
            self.log.info(f"<<< Load finished.")
