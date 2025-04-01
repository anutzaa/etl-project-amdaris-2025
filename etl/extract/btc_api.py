from datetime import datetime

import requests
import os

from utils import save_to_file
from mysql_conn import MySQLConnector


class BitcoinAPI:
    def __init__(self, api_key, conn: MySQLConnector):
        self.api_key = api_key
        self.base_url = 'https://www.alphavantage.co/query'
        self.conn = conn

    def get_bitcoin_data(self, market, function='DIGITAL_CURRENCY_DAILY'):
        params = {'function': function,
                  'symbol': 'BTC',
                  'market': market,
                  'apikey': self.api_key
                  }

        response = requests.get(self.base_url, params=params)

        response_code = response.status_code
        error_message = response.text if response_code != 200 else None

        response.raise_for_status()
        data = response.json()

        file_path, file_created_date, file_last_modified_date = save_to_file(data, 'btc')
        row_count = len(data.get("Time Series (Digital Currency Daily)", {}))

        currency_id = self.conn.get_currency_by_code(market)

        self.conn.log_import(currency_id, os.path.dirname(file_path), os.path.basename(file_path),
                             file_created_date, file_last_modified_date, row_count)

        return response_code, error_message

    def call(self):
        start_time = datetime.now()

        currencies = self.conn.get_currencies()

        for currency in currencies:
            currency_id = currency[0]
            currency_code = currency[1]

            response_code, error_message = self.get_bitcoin_data(currency_code)

            self.conn.log_api_import(currency_id, 'BTC', start_time, response_code, error_message)

        end_time = datetime.now()

        for currency in currencies:
            currency_id = currency[0]
            self.conn.log_api_import_end(currency_id, 'BTC', start_time, end_time)
