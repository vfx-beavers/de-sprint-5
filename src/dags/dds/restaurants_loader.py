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

class RestObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    update_ts: datetime

class RestaurantSTGRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_restaurants(self, ts_threshold: int, limit: int) -> List[RestObj]:
        with self._db.client().cursor(row_factory=class_row(RestObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id as restaurant_id, object_value as restaurant_name, update_ts
                    FROM stg.cs_restaurants
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


class RestaurantDDSRepository:
    def __init__(self, log: Logger) -> None:
        self.log = log

    def insert_restaurant(self, conn: Connection, rest: RestObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.cs_restaurants(restaurant_id, restaurant_name)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s)
                """,
                {
                    "restaurant_id": rest.restaurant_id,
                    "restaurant_name": rest.restaurant_name
                },
            )


class RestaurantLoader:
    WF_KEY = "cs_restaurants_stg_to_dds"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.origin  = RestaurantSTGRepository(pg_conn)
        self.stg     = RestaurantDDSRepository(log)
        self.log     = log
        self.settings_repository = StgEtlSettingsRepository("dds")

    def load_restaurants(self):

        with self.pg_conn.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.get_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} new restaurants in stg.cs_restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for restjson in load_queue:
                self.stg.insert_restaurant(conn, restjson)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([r.id for r in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

