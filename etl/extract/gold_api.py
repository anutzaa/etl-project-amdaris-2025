import requests
import os
from datetime import datetime

from utils import save_to_file, process_api_response
from mysql_conn import MySQLConnector
from logger import logger


class GoldAPI:
    def __init__(self, api_key, conn: MySQLConnector):
        self.api_key = api_key
        self.base_url = 'https://gold.g.apised.com/v1/latest'
        self.conn = conn
        logger.debug("GoldAPI client initialized")

    def get_gold_data(self, symbol):
        logger.info(f"Fetching Gold data for currency: {symbol}")
        params = {
            'metals': 'XAU',
            'base_currency': symbol,
            'weight_unit': 'gram',
        }

        headers = {'x-api-key': self.api_key}

        logger.debug(
            f"Making API request to {self.base_url} with params: {params}"
        )

        start = datetime.now()
        start_time = start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]

        response = requests.get(self.base_url, params=params, headers=headers)

        end = datetime.now()
        end_time = end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]

        response_code, error_message, data = process_api_response(response)

        logger.debug("Saving data to file")
        file_path, file_created_date, file_last_modified_date = save_to_file(
            data, 'gold'
        )
        row_count = len(data.get("data", {}).get("metal_prices", {}))
        logger.info(
            f"Saved Gold data with {row_count} gold prices to {file_path}"
        )

        currency_id = self.conn.get_currency_by_code(symbol)
        if currency_id:
            logger.debug(f"Logging import for currency_id: {currency_id}")
            self.conn.log_import(
                currency_id,
                os.path.dirname(file_path),
                os.path.basename(file_path),
                file_created_date,
                file_last_modified_date,
                row_count,
            )
        else:
            logger.error(f"Could not find currency_id for symbol: {symbol}")

        return start_time, end_time, response_code, error_message

    def call(self):
        logger.info("Starting Gold data extraction process")

        currencies = self.conn.get_currencies()
        logger.info(f"Processing {len(currencies)} currencies for Gold data")

        for currency in currencies:
            currency_id = currency[0]
            currency_code = currency[1]
            logger.info(
                f"Processing currency: {currency_code} (ID: {currency_id})"
            )

            try:
                start_time, end_time, response_code, error_message = self.get_gold_data(
                    currency_code
                )
                logger.debug(f"API response code: {response_code}")
                self.conn.log_api_import(
                    currency_id,
                    'XAU',
                    start_time,
                    end_time,
                    response_code,
                    error_message,
                )
            except Exception as e:
                logger.error(
                    f"Error processing Gold data for {currency_code}: {str(e)}",
                    exc_info=True,
                )

        logger.info("Gold data extraction process completed")
