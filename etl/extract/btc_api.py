from datetime import datetime
import requests
import os

from etl.extract.utils import save_to_file, process_api_response
from etl.extract.database_extract import DBConnectorExtract
from etl.extract.logger import logger


class BitcoinExtract:
    """
    Extracts daily Bitcoin data from the Alpha Vantage API and logs metadata to the database.

    Methods:
        __init__(conn)         -- Initializes with a DB connection
        call()                 -- Run extraction process for all currencies
        get_bitcoin_data()     -- Fetch and store Bitcoin data for a specific market

    Instance Variables:
        api_key  -- API key for Alpha Vantage
        base_url -- Endpoint URL for API requests
        conn     -- DBConnectorExtract instance for DB operations
    """

    def __init__(self, conn: DBConnectorExtract):
        """
        Initialize BitcoinExtract with database connector and API configuration.

        Parameters:
            conn -- Instance of DBConnectorExtract for database interactions
        """
        self.api_key = os.environ.get("BTC_API_KEY")
        self.base_url = "https://www.alphavantage.co/query"
        self.conn = conn
        logger.debug("BitcoinAPI client initialized")

    def get_bitcoin_data(self, market, function="DIGITAL_CURRENCY_DAILY"):
        """
        Fetch Bitcoin daily data for a given market and save it to a file.

        Parameters:
            market   -- Target currency code (e.g., 'USD')
            function -- Alpha Vantage function to call (default: 'DIGITAL_CURRENCY_DAILY')

        Returns:
            tuple -- (start_time, end_time, response_code, error_message)
        """
        logger.info(f"Fetching Bitcoin data for market: {market}")
        params = {
            "function": function,
            "symbol": "BTC",
            "market": market,
            "apikey": self.api_key,
        }

        logger.debug(
            f"Making API request to {self.base_url} with params: {params}"
        )

        start = datetime.now()
        start_time = start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]
        logger.debug(f"API request started at: {start_time}")

        try:
            response = requests.get(self.base_url, params=params)
            end = datetime.now()
            end_time = end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]
            logger.debug(f"API response received at: {end_time}")

            response_code, error_message, data = process_api_response(response)

            logger.debug("Saving data to file")
            file_path, file_created_date, file_last_modified_date = (
                save_to_file(data, "btc")
            )
            row_count = len(
                data.get("Time Series (Digital Currency Daily)", {})
            )
            logger.info(
                f"Saved {row_count} Bitcoin data points to {file_path}"
            )

            currency_id = self.conn.get_currency_by_code(market)
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
                logger.error(
                    f"Could not find currency_id for market: {market}"
                )

            return start_time, end_time, response_code, error_message

        except Exception as e:
            logger.error(
                f"Failed to fetch or process Bitcoin data: {str(e)}",
                exc_info=True,
            )
            return start_time, None, None, str(e)

    def call(self):
        """
        Fetch and log Bitcoin data for all currencies in the system.
        Loops through available currencies and logs API import details.

        Returns:
            None
        """
        logger.info("Starting Bitcoin data extraction process")

        currencies = self.conn.get_currencies()
        logger.info(
            f"Processing {len(currencies)} currencies for Bitcoin data"
        )

        for currency in currencies:
            currency_id = currency[0]
            currency_code = currency[1]
            logger.info(
                f"Processing currency: {currency_code} (ID: {currency_id})"
            )

            try:
                start_time, end_time, response_code, error_message = (
                    self.get_bitcoin_data(currency_code)
                )
                logger.debug(f"API response code: {response_code}")
                self.conn.log_api_import(
                    currency_id,
                    "BTC",
                    start_time,
                    end_time,
                    response_code,
                    error_message,
                )
            except Exception as e:
                logger.error(
                    f"Error processing Bitcoin data for {currency_code}: {str(e)}",
                    exc_info=True,
                )

        logger.info("Bitcoin data extraction process completed")
