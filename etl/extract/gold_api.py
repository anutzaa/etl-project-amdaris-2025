import requests
import os

from datetime import datetime

from utils import save_to_file
from mysql_conn import MySQLConnector


class GoldAPI:
    def __init__(self, api_key, conn: MySQLConnector):
        self.api_key = api_key
        self.base_url = 'https://gold.g.apised.com/v1/latest'
        self.conn = conn

    def get_gold_data(self, symbol):
        params = {'metals': 'XAU',
                  'base_currency': symbol,
                  'weight_unit': 'gram'
                  }

        headers = {
            'x-api-key': self.api_key
        }

        response = requests.get(self.base_url, params=params, headers=headers)
        response_code = response.status_code
        error_message = response.text if response_code != 200 else None

        response.raise_for_status()
        data = response.json()

        file_path, file_created_date, file_last_modified_date = save_to_file(data, 'gold')
        row_count = len(data.get("data", {}).get("metal_prices", {}))

        currency_id = self.conn.get_currency_by_code(symbol)

        self.conn.log_import(currency_id, os.path.dirname(file_path), os.path.basename(file_path),
                             file_created_date, file_last_modified_date, row_count)

        return response_code, error_message

    def call(self):
        start = datetime.now()
        start_time = start.strftime("%Y-%m-%d %H:%M:%S")

        currencies = self.conn.get_currencies()

        for currency in currencies:
            currency_id = currency[0]
            currency_code = currency[1]

            response_code, error_message = self.get_gold_data(currency_code)

            self.conn.log_api_import(currency_id, 'XAU', start_time, response_code, error_message)

        end = datetime.now()
        end_time = end.strftime("%Y-%m-%d %H:%M:%S")

        for currency in currencies:
            currency_id = currency[0]
            self.conn.log_api_import_end(currency_id, 'XAU', start_time, end_time)

