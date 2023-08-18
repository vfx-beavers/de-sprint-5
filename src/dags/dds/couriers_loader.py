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

class CourierObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str

class CourierSTGRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_couriers(self, ts_threshold: int, limit: int) -> List[CourierObj]:
        with self._db.client().cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id as courier_id, object_value as courier_name
                    FROM stg.cs_couriers
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


class CourierDDSRepository:
    def __init__(self, log: Logger) -> None:
        self.log = log

    def insert_entry(self, conn: Connection, courier: CourierObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.cs_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name
                },
            )


class CourierLoader:
    WF_KEY = "cs_couriers_stg_to_dds"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.origin  = CourierSTGRepository(pg_conn)
        self.stg     = CourierDDSRepository(log)
        self.log     = log
        self.settings_repository = StgEtlSettingsRepository("dds")

    def load_couriers(self):

        with self.pg_conn.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.get_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} new couriers in stg.cs_couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for c in load_queue:
                self.stg.insert_entry(conn, c)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([r.id for r in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

