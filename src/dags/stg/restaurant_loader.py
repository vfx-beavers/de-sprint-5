from logging import Logger
from typing import List

from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import json
import datetime
from wsgiref import headers
import requests

from stg.courier_system_api_caller import CourierSystem


class RestaurantRepository:

    def insert_entry(self, conn: Connection, obj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.cs_restaurants(object_id, object_value, update_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO NOTHING;
                """,
                {
                    "object_id": obj["_id"],
                    "object_value": obj["name"],
                    "update_ts": datetime.datetime.now()
                },
            )


class RestaurantLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierSystem(log)
        self.stg = RestaurantRepository()
        self.log = log

    def load_restaurants(self):

        with self.pg_dest.connection() as conn:

            i = 1
            limit  = 50
            offset = 0

            while True:

                load_queue = self.origin.get_restaurants_from_api(limit, offset)

                self.log.info(f"Reveived {i} batch of {len(load_queue)} restaurants to load.")
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


            self.log.info(f"<<< Load finished.")
