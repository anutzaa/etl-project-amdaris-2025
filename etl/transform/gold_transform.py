import os
from datetime import datetime

from etl.transform.database_transform import DBConnectorTransform
from etl.transform.utils_transform import (
    move_file,
    load_json_file,
)
from etl.transform.logger_transform import logger


class GoldTransform:
    """
    Handles transformation of raw Gold API data files into structured records
    and loads them into the database.

    Methods:
        __init__()   -- Initializes with a DB connection
        transform()  -- Processes and loads data from a single file
        call()       -- Triggers processing of all files based on import_log

    Instance Variables:
        conn      -- Database connection object (DBConnectorTransform)

    """

    def __init__(self, conn: DBConnectorTransform):
        """
        Initialize the transformer with a database connection.

        Parameters:
            conn -- DBConnectorTransform instance for DB operations
        """
        self.conn = conn

    def transform(self, file_info):
        """
        Process a single Gold data file, transform its contents, and insert into DB.

        Parameters:
            file_info -- Dictionary containing file information from import_log

        Returns:
            None
        """
        file_path = file_info['full_path']
        import_log_id = file_info['id']

        logger.info(f"Processing gold file: {file_path} (import_log_id: {import_log_id})")
        data_type = "gold"
        status = "error"
        processed_count = 0
        currency_id = file_info['currency_id']

        try:
            data_list = load_json_file(file_path)

            if data_list is None:
                return

            for data_obj in data_list:
                if (
                        data_obj.get("status") != "success"
                        or "data" not in data_obj
                ):
                    logger.warning(f"Invalid data format in file: {file_path}")
                    continue

                data = data_obj["data"]

                base_currency = data.get("base_currency", "")
                if not base_currency:
                    logger.warning(
                        f"No base currency found in file: {file_path}"
                    )
                    continue

                logger.debug(
                    f"Processing data for base currency: {base_currency}"
                )

                if not currency_id:
                    currency_id = self.conn.get_currency_by_code(base_currency)

                if not currency_id:
                    logger.warning(
                        f"No currency ID found for code: {base_currency}"
                    )
                    continue

                timestamp_ms = data.get("timestamp")
                if not timestamp_ms:
                    logger.warning(f"No timestamp found in file: {file_path}")
                    continue

                date = datetime.fromtimestamp(timestamp_ms / 1000).date()
                logger.debug(f"Processing data for date: {date}")

                metal_prices = data.get("metal_prices", {}).get("XAU", {})
                if not metal_prices:
                    logger.warning(
                        f"No XAU metal prices found in file: {file_path}"
                    )
                    continue

                currency_rates = data.get("currency_rates", {})
                if not currency_rates:
                    logger.warning(
                        f"No currency rates found in file: {file_path}"
                    )
                    continue

                try:
                    open_price = float(metal_prices.get("open"))
                    high_price = float(metal_prices.get("high"))
                    low_price = float(metal_prices.get("low"))
                    price = float(metal_prices.get("price"))
                    price_24k = float(metal_prices.get("price_24k"))
                    price_18k = float(metal_prices.get("price_18k"))
                    price_14k = float(metal_prices.get("price_14k"))

                    rate_data = {}
                    for currency_code, rate_value in currency_rates.items():
                        try:
                            rate_data[currency_code.upper()] = float(
                                rate_value
                            )
                        except (ValueError, TypeError):
                            logger.warning(
                                f"Invalid rate value for {currency_code}: {rate_value}"
                            )

                    logger.debug(f"Found {len(rate_data)} currency rates")

                    logger.debug(
                        f"Upserting gold data for currency_id {currency_id}, date {date}"
                    )
                    result = self.conn.upsert_gold_data(
                        currency_id,
                        date,
                        open_price,
                        high_price,
                        low_price,
                        price,
                        price_24k,
                        price_18k,
                        price_14k,
                        rate_data,
                    )

                    if result:
                        processed_count += 1

                except Exception as e:
                    logger.error(
                        f"Error processing gold data: {str(e)}", exc_info=True
                    )

            if processed_count > 0:
                status = "processed"
                logger.info(
                    f"Successfully processed {processed_count} data points from file: {file_path}"
                )
            else:
                logger.warning(f"No data processed from file: {file_path}")

        except Exception as e:
            logger.error(
                f"Error processing file {file_path}: {str(e)}", exc_info=True
            )
            status = "error"

        new_file_path = move_file(status, data_type, file_path)

        logger.info(f"Logging transformation for file {file_path}")
        self.conn.log_transform(
            currency_id,
            os.path.dirname(new_file_path),
            os.path.basename(new_file_path),
            processed_count,
            status
        )

    def call(self):
        """
        Trigger the transformation process for gold files based on import_log.

        Returns:
            None
        """
        logger.info("Starting Gold transformation based on import_log")

        files_to_process = self.conn.get_files_to_process("gold")

        if not files_to_process:
            logger.info("No gold files found in import_log to process")
            return

        logger.info(f"Processing {len(files_to_process)} gold files")

        for file_info in files_to_process:
            self.transform(file_info)

        logger.info("Gold transformation complete")
