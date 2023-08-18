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

class TsObj(BaseModel):
    id: int
    order_ts: datetime

class TsSTGRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_ts(self, ts_threshold: int, limit: int) -> List[TsObj]:
        with self._db.client().cursor(row_factory=class_row(TsObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_ts
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

class TsDDSRepository:
    def __init__(self, log: Logger):
        self.log = log

    def insert_ts(self, conn: Connection, tsobj: TsObj) -> None:

        # tsms = datetime.strptime(tsjson["update_ts"], '%Y-%m-%d %H:%M:%S.%f')
        ts = tsobj.order_ts.replace(microsecond=0)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.cs_timestamps(ts, date, year, month, day, time)
                    VALUES (%(ts)s, %(date)s, %(year)s, %(month)s, %(day)s, %(time)s)
                """,
                {
                    "ts": tsobj.order_ts,
                    "date":  ts.date(),
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "time": ts.time()
                },
            )

class TsLoader:
    WF_KEY = "cs_timestamps_stg_to_dds"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.origin  = TsSTGRepository(pg_conn)
        self.stg     = TsDDSRepository(log)
        self.log     = log
        self.settings_repository = StgEtlSettingsRepository("dds")

    def load_timestamps(self):

        with self.pg_conn.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.get_ts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} new timestamps in stg.cs_deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for ts in load_queue:
                self.stg.insert_ts(conn, ts)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings) 
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
