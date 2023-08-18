from logging import Logger
from typing import List

from lib.dict_util import json2str
import json
from wsgiref import headers
import requests

class CourierSystem:

    def __init__(self, log: Logger) -> None:
        self.log = log
        self.base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
        self.headers = {
                "X-Nickname" : 'cgbeavers2',
                "X-Cohort" : '15',
                "X-API-KEY" : '25c27781-8fde-4b30-a22e-524044a7580f'
        }

    def get_couriers_from_api(self, limit, offset) -> List:
        params = {
            "sort_field" : "name", 
            "sort_direction" : "asc",
            "limit" : limit,
            "offset" : offset
            }

        r = requests.get(
            url     = self.base_url + '/couriers', 
            params  = params, 
            headers = self.headers)
        
        json_record = json.loads(r.content)
        return json_record
    
    def get_restaurants_from_api(self, limit, offset) -> List:
        params = {
            "sort_field" : "name", 
            "sort_direction" : "asc",
            "limit" : limit,
            "offset" : offset
            }

        r = requests.get(
            url     = self.base_url + '/restaurants', 
            params  = params, 
            headers = self.headers)
        
        json_record = json.loads(r.content)

        return json_record


    def get_deliveries_from_api(self, limit, offset, from_ts) -> List:
        params = {
            "sort_field" : "date", 
            "sort_direction" : "asc",
            "from": from_ts,
            "limit" : limit,
            "offset" : offset
            }

        r = requests.get(
            url     = self.base_url + '/deliveries', 
            params  = params, 
            headers = self.headers)
        
        json_record = json.loads(r.content)

        return json_record
